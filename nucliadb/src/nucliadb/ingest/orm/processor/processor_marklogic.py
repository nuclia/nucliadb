import asyncio
import hashlib
import logging
from typing import Any, Iterator, cast
from uuid import uuid4

import aiohttp.client_exceptions
import nats.errors
import nats.js.errors
from nidx_protos import noderesources_pb2 as brain

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.resources import ResourceContent, ResourceCreateContent
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.exceptions import ConflictError, MaindbServerError
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.marklogic import documents
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.fields.conversation import PAGE_SIZE
from nucliadb.ingest.orm.exceptions import (
    DeadletteredError,
    InvalidBrokerMessage,
    SequenceOrderViolation,
)
from nucliadb.ingest.orm.index_message import FieldData, IndexMessageBuilder, ResourceData
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb.ingest.orm.processor import sequence_manager
from nucliadb.ingest.orm.processor.auditing import collect_audit_fields
from nucliadb.ingest.orm.processor.data_augmentation import (
    get_generated_fields,
    send_generated_fields_to_process_v2,
)
from nucliadb.ingest.orm.processor.processor import MESSAGE_TO_NOTIFICATION_SOURCE
from nucliadb.ingest.orm.resource import (
    MAX_FIELD_STATUS_ERRORS,
    Resource,
)
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_protos import (
    knowledgebox_pb2,
    resources_pb2,
    writer_pb2,
)
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage

METADATA_STATUS_PB_TYPE_TO_NAME_MAP = {
    writer_pb2.Metadata.Status.ERROR: ResourceProcessingStatus.ERROR.name,
    writer_pb2.Metadata.Status.PROCESSED: ResourceProcessingStatus.PROCESSED.name,
    writer_pb2.Metadata.Status.PENDING: ResourceProcessingStatus.PENDING.name,
    writer_pb2.Metadata.Status.BLOCKED: ResourceProcessingStatus.BLOCKED.name,
    writer_pb2.Metadata.Status.EXPIRED: ResourceProcessingStatus.EXPIRED.name,
}


FieldContent = datamanagers.fields.FieldContent
FieldProcessedContent = datamanagers.fields.FieldProcessedContent
ChunkContent = datamanagers.fields.ChunkContent
ChunkEmbeddingContent = datamanagers.fields.ChunkEmbeddingContent
ConversationPageContent = datamanagers.conversations.ConversationPageContent


logger = logging.getLogger("ingest-processor")


class ProcessorMarkLogic:
    def __init__(
        self,
        driver: MarkLogicDriver,
        storage: Storage,
        pubsub: PubSubDriver | None = None,
        partition: str | None = None,
    ):
        self.driver = driver
        self.storage = storage
        self.partition = partition
        self.pubsub = pubsub

    async def get_kb_obj(
        self, txn: Transaction, kbid: knowledgebox_pb2.KnowledgeBoxID
    ) -> KnowledgeBox | None:
        uuid: str | None = kbid.uuid
        if uuid == "":
            uuid = await datamanagers.kb.get_kbid(txn, slug=kbid.slug)

        if uuid is None:
            return None

        if not (await datamanagers.kb.exists(txn, kbid=uuid)):
            return None

        storage = await get_storage()
        kbobj = KnowledgeBox(txn, storage, uuid)
        return kbobj

    async def get_extended_audit_data(self, message: writer_pb2.BrokerMessage) -> writer_pb2.Audit:
        message_audit = writer_pb2.Audit()
        message_audit.CopyFrom(message.audit)
        message_audit.kbid = message.kbid
        message_audit.uuid = message.uuid
        message_audit.message_source = message.source
        message_audit.field_metadata.extend([fcmw.field for fcmw in message.field_metadata])
        audit_fields = await collect_audit_fields(self.driver, self.storage, message)
        message_audit.audit_fields.extend(audit_fields)
        return message_audit

    async def notify_commit(
        self,
        *,
        partition: str,
        seqid: int,
        message: writer_pb2.BrokerMessage,
        write_type: writer_pb2.Notification.WriteType.ValueType,
    ):
        message_audit = await self.get_extended_audit_data(message)
        notification = writer_pb2.Notification(
            partition=int(partition),
            seqid=seqid,
            multi="",
            uuid=message.uuid,
            kbid=message.kbid,
            action=writer_pb2.Notification.Action.COMMIT,
            write_type=write_type,
            source=MESSAGE_TO_NOTIFICATION_SOURCE[message.source],
            processing_errors=len(message.errors) > 0,
            message_audit=message_audit,
        )

        await self.notify(
            const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=message.kbid),
            notification.SerializeToString(),
        )

    async def notify_abort(
        self,
        *,
        partition: str,
        seqid: int,
        kbid: str,
        rid: str,
        source: writer_pb2.BrokerMessage.MessageSource.ValueType,
    ):
        message = writer_pb2.Notification(
            partition=int(partition),
            seqid=seqid,
            multi="",
            uuid=rid,
            kbid=kbid,
            action=writer_pb2.Notification.ABORT,
            source=MESSAGE_TO_NOTIFICATION_SOURCE[source],
        )
        await self.notify(
            const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=kbid),
            message.SerializeToString(),
        )

    async def notify(self, channel, payload: bytes):
        if self.pubsub is not None:
            await self.pubsub.publish(channel, payload)

    async def process(
        self,
        message: writer_pb2.BrokerMessage,
        seqid: int,
        partition: str | None = None,
        transaction_check: bool = True,
    ) -> None:
        partition = partition if self.partition is None else self.partition
        if partition is None:
            raise RuntimeError("Can't process message from unknown partition")

        if transaction_check:
            last_seqid = await sequence_manager.get_last_seqid(self.driver, partition)
            if last_seqid is not None and seqid <= last_seqid:
                raise SequenceOrderViolation(last_seqid)

        if message.type == writer_pb2.BrokerMessage.MessageType.DELETE:
            await self.delete(message, seqid, partition, transaction_check)
        elif message.type == writer_pb2.BrokerMessage.MessageType.AUTOCOMMIT:
            await self.txn(message, seqid, partition, transaction_check)
        else:  # pragma: no cover
            logger.error(
                f"Unsupported message type: {message.type}",
                extra={"seqid": seqid, "partition": partition},
            )

    @processor_observer.wrap({"type": "delete"})
    async def delete(
        self,
        message: writer_pb2.BrokerMessage,
        seqid: int,
        partition: str,
        transaction_check: bool = True,
    ) -> None:
        try:
            kbid, uuid = await self.ensure_kb_and_resource(message.kbid, message.uuid, message.slug)
        except (KnowledgeBoxDoesNotExist, ResourceDoesNotExist):
            if transaction_check:
                await self.set_last_seqid(partition, seqid)
            return None

        async with self.driver.transaction(read_only=False, kbid=kbid) as txn:
            logger.info("Deleting resource", extra={"kbid": kbid, "rid": uuid})
            try:
                await datamanagers.resources.delete_resource(txn, kbid=kbid, rid=uuid)
                await txn.commit()
                if transaction_check:
                    await self.set_last_seqid(partition, seqid)
            except Exception as exc:
                await txn.abort()
                await self.notify_abort(
                    partition=partition,
                    seqid=seqid,
                    kbid=kbid,
                    rid=uuid,
                    source=message.source,
                )
                raise exc
        await self.notify_commit(
            partition=partition,
            seqid=seqid,
            message=message,
            write_type=writer_pb2.Notification.WriteType.DELETED,
        )

    async def set_last_seqid(self, partition: str, seqid: int) -> None:
        async with self.driver.transaction(read_only=False, system=True) as txn:
            await sequence_manager.set_last_seqid(txn, partition, seqid)
            await txn.commit()

    async def ensure_kb_and_resource(
        self,
        kbid: str,
        uuid: str,
        slug: str,
    ) -> tuple[str, str]:
        if not await datamanagers.kb.exists_by_id(self.driver, kbid=kbid):
            raise KnowledgeBoxDoesNotExist(f"Knowledge Box with ID {kbid} does not exist.")
        if not uuid:
            uuid_from_slug = await datamanagers.resources.get_resource_rid_by_slug(
                self.driver, kbid=kbid, slug=slug
            )
            if uuid_from_slug is None:
                raise ResourceDoesNotExist(f"Resource with slug {slug} does not exist in KB {kbid}.")
            uuid = uuid_from_slug
        return kbid, uuid

    async def _mark_resource_error(self, kbid: str, uuid: str) -> None:
        # TODO(Marklogic): Update resource status to error
        return

    async def deadletter(self, message: writer_pb2.BrokerMessage, partition: str, seqid: int) -> None:
        await self.storage.deadletter(message, 0, seqid, partition)

    @processor_observer.wrap({"type": "handle_data_augmentation_fields"})
    async def handle_data_augmentation_fields(
        self, message: writer_pb2.BrokerMessage, kbid: str, rid: str
    ):
        """
        Fields generated by data augmentation tasks are sent back to the processor to be processed and indexed.
        """
        generated_fields = await get_generated_fields(message)
        if generated_fields.is_not_empty():
            await send_generated_fields_to_process_v2(kbid, rid, generated_fields, message)

    async def create_resource(
        self,
        txn: MarkLogicTransaction,
        broker_message: writer_pb2.BrokerMessage,
        kbid: str,
        uuid: str,
        index_message: brain.Resource,
    ) -> None:
        assert broker_message.HasField("basic")
        basic = broker_message.basic

        def get_title(index_message: brain.Resource, basic: writer_pb2.Basic) -> str:
            title_field_id = "a/title"
            if title_field_id in index_message.texts:
                return index_message.texts[title_field_id].text
            return basic.title or basic.slug

        origin = broker_message.origin if broker_message.HasField("origin") else None
        created_at = (
            index_message.metadata.created.ToJsonString()
            if index_message.metadata.HasField("created")
            else origin.created.ToJsonString()
            if origin is not None and origin.HasField("created")
            else basic.created.ToJsonString()
            if basic.HasField("created")
            else ""
        )
        modified_at = (
            index_message.metadata.modified.ToJsonString()
            if index_message.metadata.HasField("modified")
            else origin.modified.ToJsonString()
            if origin is not None and origin.HasField("modified")
            else basic.modified.ToJsonString()
            if basic.HasField("modified")
            else ""
        )
        content = ResourceCreateContent(
            slug=basic.slug or uuid,
            basic=documents.to_json(basic),
            origin=documents.to_json(broker_message.origin)
            if broker_message.HasField("origin")
            else None,
            extra=documents.to_json(broker_message.extra) if broker_message.HasField("extra") else None,
            security=documents.to_json(broker_message.security)
            if broker_message.HasField("security")
            else None,
            title=get_title(index_message, basic),
            created_at=created_at,
            modified_at=modified_at,
            processing_status=brain.Resource.ResourceStatus.Name(index_message.status),
            facets=list(index_message.labels),
        )
        # TODO(Marklogic): origin relation triplets
        # TODO(Marklogic): user relation triplets
        await datamanagers.resources.create_resource(txn, kbid=kbid, rid=uuid, data=content)

        # User relations are stored in the object store
        if broker_message.HasField("user_relations"):
            orm_resource = await self.get_orm_resource(txn, kbid, uuid)
            await orm_resource.set_user_relations(broker_message.user_relations)

    async def get_orm_resource(self, txn: MarkLogicTransaction, kbid: str, uuid: str) -> Resource:
        return Resource(
            txn=txn, storage=await get_storage(), kbid=kbid, uuid=uuid, disable_vectors=False
        )

    async def create_fields(
        self,
        txn: MarkLogicTransaction,
        message: writer_pb2.BrokerMessage,
        kbid: str,
        uuid: str,
        index_message: brain.Resource,
    ) -> None:
        def get_text_md5(text: writer_pb2.FieldText) -> str | None:
            if text.md5:
                return text.md5
            if text.body:
                return hashlib.md5(text.body.encode(), usedforsecurity=False).hexdigest()
            return None

        def get_field_status(
            field_type: resources_pb2.FieldType.ValueType, field: str
        ) -> dict[str, Any]:
            status = writer_pb2.FieldStatus()
            for error in message.errors:
                if error.field_type == field_type and error.field == field:
                    field_error = status.errors.add(source_error=error)
                    field_error.created.GetCurrentTime()

            if len(status.errors) > MAX_FIELD_STATUS_ERRORS:
                errors_to_trim = len(status.errors) - MAX_FIELD_STATUS_ERRORS
                logger.warning(
                    "Field status error limit exceeded, trimming tail errors",
                    extra={
                        "kbid": kbid,
                        "rid": uuid,
                        "field_type": field_type,
                        "field": field,
                        "stored_errors": len(status.errors),
                        "max_errors": MAX_FIELD_STATUS_ERRORS,
                        "trimmed_errors": errors_to_trim,
                    },
                )
                del status.errors[-errors_to_trim:]

            if message.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
                status.status = (
                    writer_pb2.FieldStatus.Status.ERROR
                    if any(
                        error.source_error.severity == writer_pb2.Error.Severity.ERROR
                        for error in status.errors
                    )
                    else writer_pb2.FieldStatus.Status.PROCESSED
                )
            else:
                field_status = next(
                    (
                        field_status.status
                        for field_status in message.field_statuses
                        if field_status.id.field_type == field_type and field_status.id.field == field
                    ),
                    None,
                )
                if field_status is not None:
                    status.status = field_status

            return documents.to_json(status)

        # Persist field values (ORM) in Marklogic
        for field, text in message.texts.items():
            field_content = FieldContent(
                resource_id=uuid,
                field_id=field,
                field_type="t",
                value=documents.to_json(text),
                md5=get_text_md5(text),
                status=get_field_status(resources_pb2.FieldType.TEXT, field),
            )
            await datamanagers.fields.create_document(txn, field_content)

        for field, link in message.links.items():
            field_content = FieldContent(
                resource_id=uuid,
                field_id=field,
                field_type="u",
                value=documents.to_json(link),
                md5=None,
                status=get_field_status(resources_pb2.FieldType.LINK, field),
            )
            await datamanagers.fields.create_document(txn, field_content)

        for field, file in message.files.items():
            field_content = FieldContent(
                resource_id=uuid,
                field_id=field,
                field_type="f",
                value=documents.to_json(file),
                md5=file.file.md5 or None,
                status=get_field_status(resources_pb2.FieldType.FILE, field),
            )
            await datamanagers.fields.create_document(txn, field_content)

        for field, kv in message.key_value_fields.items():
            field_content = FieldContent(
                resource_id=uuid,
                field_id=field,
                field_type="k",
                value=documents.to_json(kv),
                md5=None,
                status=get_field_status(resources_pb2.FieldType.KEY_VALUE, field),
            )
            await datamanagers.fields.create_document(txn, field_content)

        for field, conversation in message.conversations.items():
            status = get_field_status(resources_pb2.FieldType.CONVERSATION, field)
            await self.create_conversation(txn, kbid, uuid, field, conversation, status)

        # Persist processed data for search alongside the source field values.
        for fpc, chunks, embeddings in self.get_field_processed_contents(index_message):
            await datamanagers.fields.create_processed_documents(
                txn=txn,
                field=fpc,
                chunks=chunks,
                embeddings=embeddings,
                skip_field_texts=index_message.skip_texts,
                skip_chunks=index_message.skip_paragraphs,
            )

        # Persist processed metadata in object storage
        orm_resource = await self.get_orm_resource(txn, kbid, uuid)
        await orm_resource.apply_field_extracted_data(message)

    def get_field_processed_contents(
        self,
        index_message: brain.Resource,
    ) -> Iterator[tuple[FieldProcessedContent, list[ChunkContent], list[ChunkEmbeddingContent]]]:
        resource_id = index_message.resource.uuid
        for fid, text_data in index_message.texts.items():
            field_type, field_id = fid.split("/")
            field_text = text_data.text
            field = FieldProcessedContent(
                resource_id=resource_id,
                field_id=field_id,
                field_type=field_type,
                extracted_text=field_text,
                facets=list(text_data.labels),
            )
            chunks = []
            embeddings = []
            field_paragraphs = index_message.paragraphs.get(fid) or brain.IndexParagraphs()
            for paragraph_id, paragraph_data in field_paragraphs.paragraphs.items():
                paragaph_text = field_text[paragraph_data.start : paragraph_data.end]
                chunks.append(
                    ChunkContent(
                        resource_id=resource_id,
                        field_id=field_id,
                        field_type=field_type,
                        chunk_id=paragraph_id,
                        text=paragaph_text,
                        facets=list(paragraph_data.labels),
                        data=documents.to_json(paragraph_data.metadata),
                    ),
                )
                for vectorset_id, embeddings_data in paragraph_data.vectorsets_sentences.items():
                    for embedding_index, (embedding_key, embedding_data) in enumerate(
                        embeddings_data.sentences.items()
                    ):
                        embeddings.append(
                            ChunkEmbeddingContent(
                                resource_id=resource_id,
                                field_id=field_id,
                                field_type=field_type,
                                chunk_id=embedding_key,
                                model_id=vectorset_id,
                                chunk_embedding_index=embedding_index,
                                vector=list(embedding_data.vector),
                                data=documents.to_json(embedding_data.metadata),
                            )
                        )
            yield field, chunks, embeddings

    async def create_conversation(
        self,
        txn: MarkLogicTransaction,
        kbid: str,
        uuid: str,
        field_id: str,
        conversation: writer_pb2.Conversation,
        status: dict[str, Any],
    ) -> None:
        metadata = resources_pb2.FieldConversation(
            size=PAGE_SIZE,
            pages=max(1, (len(conversation.messages) + PAGE_SIZE - 1) // PAGE_SIZE),
            total=len(conversation.messages),
            extract_strategy=conversation.extract_strategy,
            split_strategy=conversation.split_strategy,
            generated_by=conversation.generated_by,
        )
        splits_metadata = resources_pb2.SplitsMetadata()

        for message in conversation.messages:
            attachments = []
            for attachment_index, attachment in enumerate(message.content.attachments):
                if self.storage.needs_move(attachment, kbid):
                    message_ident = message.ident or uuid4().hex
                    storage_field = self.storage.conversation_field_attachment(
                        kbid, uuid, field_id, message_ident, attachment_index=attachment_index
                    )
                    attachment = await self.storage.normalize_binary(attachment, storage_field)
                attachments.append(attachment)
            message.content.ClearField("attachments")
            message.content.attachments.extend(attachments)

        for page_number in range(1, metadata.pages + 1):
            start = (page_number - 1) * PAGE_SIZE
            page_messages = conversation.messages[start : start + PAGE_SIZE]
            for message in page_messages:
                splits_metadata.metadata.get_or_create(message.ident).page = page_number
            page_content = ConversationPageContent(
                resource_id=uuid,
                field_id=field_id,
                conversation_page=page_number,
                value=documents.to_json(resources_pb2.Conversation(messages=page_messages)),
            )
            await datamanagers.conversations.create_page(txn, page_content)

        await datamanagers.conversations.create_page(
            txn,
            ConversationPageContent(
                resource_id=uuid,
                field_id=field_id,
                conversation_page=0,
                value=documents.to_json(splits_metadata),
            ),
        )
        await datamanagers.fields.create_document(
            txn,
            FieldContent(
                resource_id=uuid,
                field_id=field_id,
                field_type="c",
                value=documents.to_json(metadata),
                md5=None,
                status=status,
            ),
        )

    async def update_resource(
        self,
        txn: MarkLogicTransaction,
        message: writer_pb2.BrokerMessage,
        resource: ResourceContent,
        kbid: str,
        uuid: str,
    ) -> None: ...

    async def update_fields(
        self,
        txn: MarkLogicTransaction,
        message: writer_pb2.BrokerMessage,
        kbid: str,
        uuid: str,
    ) -> None: ...

    @processor_observer.wrap({"type": "generate_index_message"})
    async def generate_index_message(
        self,
        txn: MarkLogicTransaction,
        resource: ResourceContent | None,
        uuid: str,
        message: writer_pb2.BrokerMessage,
    ) -> brain.Resource:
        resource_created = resource is None
        orm_resource = Resource(
            txn=txn,
            storage=await get_storage(),
            kbid=message.kbid,
            uuid=uuid,
            disable_vectors=False,
        )
        builder = IndexMessageBuilderV2(orm_resource)
        if message.source == writer_pb2.BrokerMessage.MessageSource.WRITER:
            return await builder.for_writer_bm(message, resource_created)
        elif message.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
            return await builder.for_processor_bm(message)
        else:  # pragma: no cover
            raise InvalidBrokerMessage(f"Unknown broker message source: {message.source}")

    @processor_observer.wrap({"type": "txn"})
    async def txn(
        self,
        message: writer_pb2.BrokerMessage,
        seqid: int,
        partition: str,
        transaction_check: bool = True,
    ) -> None:
        try:
            kbid, uuid = await self.ensure_kb_and_resource(
                kbid=message.kbid,
                uuid=message.uuid,
                slug=message.slug,
            )
        except (KnowledgeBoxDoesNotExist, ResourceDoesNotExist):
            if transaction_check:
                await self.set_last_seqid(partition, seqid)
            return None

        async with self.driver.transaction(read_only=False, kbid=kbid) as txn:
            try:
                resource = await datamanagers.resources.get_resource(txn, kbid=kbid, rid=uuid)
                is_new_resource = resource is None
                if message.source not in (
                    writer_pb2.BrokerMessage.MessageSource.WRITER,
                    writer_pb2.BrokerMessage.MessageSource.PROCESSOR,
                ):
                    raise InvalidBrokerMessage(f"Unknown broker message source: {message.source}")

                if message.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
                    if is_new_resource:
                        logger.info(
                            f"Processor message for resource received but the resource does not exist, ignoring.",
                            extra={
                                "kbid": kbid,
                                "rid": uuid,
                                "seqid": seqid,
                            },
                        )
                        return None
                    # Handle data augmentation fields for existing resources
                    await self.handle_data_augmentation_fields(message, kbid, uuid)

                im = await self.generate_index_message(txn, None, uuid, message)
                if resource is None:
                    await self.create_resource(txn, message, kbid, uuid, im)
                    await self.create_fields(txn, message, kbid, uuid, im)
                else:
                    await self.update_fields(txn, message, kbid, uuid)
                    await self.update_resource(txn, message, resource, kbid, uuid)
                if transaction_check:
                    await self.set_last_seqid(partition, seqid)

                await txn.commit()
                await self.notify_commit(
                    partition=partition,
                    seqid=seqid,
                    message=message,
                    write_type=(
                        writer_pb2.Notification.WriteType.CREATED
                        if is_new_resource
                        else writer_pb2.Notification.WriteType.MODIFIED
                    ),
                )
            except (
                asyncio.TimeoutError,
                asyncio.CancelledError,
                aiohttp.client_exceptions.ClientError,
                ConflictError,
                MaindbServerError,
                nats.errors.NoRespondersError,
                nats.js.errors.NoStreamResponseError,
            ):  # pragma: no cover
                # Unhandled exceptions here that should bubble and hard fail
                # XXX We swallow too many exceptions here!
                await self.notify_abort(
                    partition=partition,
                    seqid=seqid,
                    kbid=kbid,
                    rid=uuid,
                    source=message.source,
                )
                raise
            except Exception as exc:
                # As we are in the middle of a transaction, we cannot let the exception raise directly
                # as we need to do some cleanup. The exception will be reraised at the end of the function
                # and then handled by the top caller, so errors can be handled in the same place.
                logger.error(
                    "Error processing message. Broker message stored in dead letter bucket.",
                    exc_info=True,
                    extra={"kbid": kbid, "rid": uuid, "seqid": seqid, "partition": partition},
                )
                await self.deadletter(message, partition, seqid)
                await self.notify_abort(
                    partition=partition,
                    seqid=seqid,
                    kbid=kbid,
                    rid=uuid,
                    source=message.source,
                )
                handled_exception = exc
            finally:
                # txn should be already commited or aborted, but in the event of an exception
                # it could be left open. Make sure to close it if it's still open
                if txn.open:
                    await txn.abort()

            if handled_exception is not None:
                if seqid == -1:
                    raise handled_exception
                else:
                    if not is_new_resource:
                        await self._mark_resource_error(kbid, uuid)
                    raise DeadletteredError() from handled_exception

            return None


class KnowledgeBoxDoesNotExist(Exception):
    """Exception raised when a Knowledge Box does not exist."""


class ResourceDoesNotExist(Exception):
    """Exception raised when a Resource does not exist."""


def get_bm_modified_fields(message: writer_pb2.BrokerMessage) -> list[writer_pb2.FieldID]:
    modified = set()
    # Added or modified fields need indexing
    for link in message.links:
        modified.add((link, writer_pb2.FieldType.LINK))
    for file in message.files:
        modified.add((file, writer_pb2.FieldType.FILE))
    for conv in message.conversations:
        modified.add((conv, writer_pb2.FieldType.CONVERSATION))
    for text in message.texts:
        modified.add((text, writer_pb2.FieldType.TEXT))
    for kv in message.key_value_fields:
        modified.add((kv, writer_pb2.FieldType.KEY_VALUE))
    if message.HasField("basic"):
        # Add title and summary only if they have changed
        if message.basic.title != "":
            modified.add(("title", writer_pb2.FieldType.GENERIC))
        if message.basic.summary != "":
            modified.add(("summary", writer_pb2.FieldType.GENERIC))

    # Deleted splits also need reindexing of the field
    for ds in message.delete_splits:
        modified.add((ds.field.field, ds.field.field_type))

    if message.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
        # Messages with field metadata, extracted text or field vectors need indexing
        for fm in message.field_metadata:
            modified.add((fm.field.field, fm.field.field_type))
        for et in message.extracted_text:
            modified.add((et.field.field, et.field.field_type))
        for fv in message.field_vectors:
            modified.add((fv.field.field, fv.field.field_type))

    if message.source == writer_pb2.BrokerMessage.MessageSource.WRITER:
        # Any field that has fieldmetadata annotations should be considered as modified
        # and needs to be reindexed
        if message.HasField("basic"):
            for ufm in message.basic.fieldmetadata:
                modified.add((ufm.field.field, ufm.field.field_type))

    return [writer_pb2.FieldID(field=field, field_type=field_type) for field, field_type in modified]


class IndexMessageBuilderV2(IndexMessageBuilder):
    @property
    def txn(self) -> MarkLogicTransaction:
        return cast(MarkLogicTransaction, self.resource.txn)

    @property
    def kbid(self) -> str:
        return self.resource.kbid

    @property
    def resource_id(self) -> str:
        return self.resource.uuid

    async def get_resource_data(self) -> ResourceData:
        content = await datamanagers.resources.get_resource(
            self.txn, kbid=self.kbid, rid=self.resource_id
        )
        assert content is not None
        user_relations = await self.resource.get_user_relations()
        return ResourceData(
            basic=documents.from_json(content.basic, writer_pb2.Basic),
            user_relations=user_relations,
            origin=documents.from_json(content.origin or {}, writer_pb2.Origin),
            security=documents.from_json(content.security or {}, resources_pb2.Security),
        )

    async def get_resource_fields(self) -> list[tuple[writer_pb2.FieldType.ValueType, str]]:
        fields = await datamanagers.fields.list_resource_documents(
            self.txn, resource_id=self.resource_id
        )
        return fields

    async def get_field_data(self, field_id: writer_pb2.FieldID) -> FieldData:
        field = await self.resource.get_field(field_id.field, field_id.field_type, load=False)
        et = await field.get_extracted_text()
        fm = await field.get_field_metadata()
        value = await self.get_field_value(field_id)
        return FieldData(
            extracted_text=et,
            field_computed_metadata=fm,
            generated_by=value.generated_by,
        )

    async def get_field_value(self, fieldid: writer_pb2.FieldID) -> Any:
        content = await datamanagers.fields.get_document(
            self.txn,
            resource_id=self.resource_id,
            field_id=fieldid.field,
            field_type=from_proto.field_type_name(fieldid.field_type).abbreviation(),
        )
        assert content is not None
        assert content.value is not None
        pb_class = {
            writer_pb2.FieldType.TEXT: writer_pb2.FieldText,
            writer_pb2.FieldType.FILE: writer_pb2.FieldFile,
            writer_pb2.FieldType.LINK: writer_pb2.FieldLink,
            writer_pb2.FieldType.KEY_VALUE: writer_pb2.FieldKeyValue,
            writer_pb2.FieldType.CONVERSATION: writer_pb2.FieldConversation,
        }[fieldid.field_type]
        return documents.from_json(content.value, pb_class)
