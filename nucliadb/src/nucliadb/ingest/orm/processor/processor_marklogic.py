import asyncio
import hashlib
import json
import logging
from collections import defaultdict
from typing import Any
from uuid import uuid4

import aiohttp.client_exceptions
import nats.errors
import nats.js.errors
from pydantic import BaseModel

from nucliadb.common.maindb.exceptions import ConflictError, MaindbServerError
from nucliadb.common.maindb.marklogic import (
    MarkLogicCollections,
    MarkLogicDriver,
    MarkLogicIndexPaths,
    MarkLogicTransaction,
)
from nucliadb.common.marklogic.document_deletion import DocumentDeletion
from nucliadb.ingest.fields.conversation import PAGE_SIZE
from nucliadb.ingest.orm.exceptions import (
    DeadletteredError,
    InvalidBrokerMessage,
    SequenceOrderViolation,
)
from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb.ingest.orm.processor import sequence_manager
from nucliadb.ingest.orm.processor.auditing import collect_audit_fields
from nucliadb.ingest.orm.processor.data_augmentation import (
    get_generated_fields,
    send_generated_fields_to_process_v2,
)
from nucliadb.ingest.orm.processor.marklogic import documents
from nucliadb.ingest.orm.processor.processor import MESSAGE_TO_NOTIFICATION_SOURCE
from nucliadb.ingest.orm.resource import MAX_FIELD_STATUS_ERRORS, Resource
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_protos import (
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


class ResourceContent(BaseModel):
    slug: str

    # Main protobuf content sections
    basic: dict[str, Any]
    origin: dict[str, Any]
    extra: dict[str, Any]
    security: dict[str, Any]

    # Other metadata sections
    processing_status: str
    created_at: str
    modified_at: str
    facets: list[str]


class ResourceCreateContent(BaseModel):
    slug: str

    basic: dict[str, Any]
    origin: dict[str, Any] | None = None
    extra: dict[str, Any] | None = None
    security: dict[str, Any] | None = None

    processing_status: str
    created_at: str
    modified_at: str
    facets: list[str] = []


class FieldContent(BaseModel):
    id: str
    type: str

    # The actual protobuf content serialized to json
    value: dict[str, Any]

    md5: str | None = None
    status: dict[str, Any] | None = None


class ConversationPageContent(BaseModel):
    id: str
    type: str
    page: int
    value: dict[str, Any]


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
            await self.resource_delete(message, seqid, partition, transaction_check)
        elif message.type == writer_pb2.BrokerMessage.MessageType.AUTOCOMMIT:
            await self.txn(message, seqid, partition, transaction_check)
        else:  # pragma: no cover
            logger.error(
                f"Unsupported message type: {message.type}",
                extra={"seqid": seqid, "partition": partition},
            )

    @processor_observer.wrap({"type": "resource_delete"})
    async def resource_delete(
        self,
        message: writer_pb2.BrokerMessage,
        seqid: int,
        partition: str,
        transaction_check: bool = True,
    ) -> None:
        try:
            kbid, uuid = await self.ensure_kb_and_slug(message.kbid, message.uuid, message.slug)
        except (KnowledgeBoxDoesNotExist, ResourceDoesNotExist):
            if transaction_check:
                await self.set_last_seqid(partition, seqid)
            return None

        async with self.driver.transaction(read_only=False, kbid=kbid) as txn:
            try:
                logger.info("Deleting resource", extra={"kbid": kbid, "rid": uuid})
                try:
                    await resource_delete(txn, kbid, uuid)
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
            finally:
                if txn.open:
                    await txn.commit()
                    if transaction_check:
                        await self.set_last_seqid(partition, seqid)
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

    async def ensure_kb_and_slug(
        self,
        kbid: str,
        uuid: str,
        slug: str,
    ) -> tuple[str, str]:
        if not await kb_exists(self.driver, kbid):
            raise KnowledgeBoxDoesNotExist(f"Knowledge Box with ID {kbid} does not exist.")
        if not uuid:
            uuid_from_slug = await resource_id_from_slug(self.driver, kbid, slug)
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
        message: writer_pb2.BrokerMessage,
        kbid: str,
        uuid: str,
    ) -> None:
        assert message.HasField("basic")
        basic = message.basic

        def get_dates(message: writer_pb2.BrokerMessage) -> tuple[str, str]:
            """
            Return the created_at and modified_at dates for the resource as a tuple of strings.

            Origin takes precedence over basic for the dates. If dates are not set, set them to the current time.
            """
            created_at = ""
            modified_at = ""

            origin = message.origin if message.HasField("origin") else None
            basic = message.basic if message.HasField("basic") else writer_pb2.Basic()

            if origin and origin.HasField("created"):
                created_at = origin.created.ToJsonString()
            elif basic.HasField("created"):
                created_at = basic.created.ToJsonString()
            else:
                basic.created.GetCurrentTime()
                created_at = basic.created.ToJsonString()
            if origin and origin.HasField("modified"):
                modified_at = origin.modified.ToJsonString()
            elif basic.HasField("modified"):
                modified_at = basic.modified.ToJsonString()
            else:
                basic.modified.GetCurrentTime()
                modified_at = basic.modified.ToJsonString()

            return created_at, modified_at

        def get_processing_status(basic: writer_pb2.Basic) -> str:
            """Get the processing status of the resource as a string."""
            return writer_pb2.Metadata.Status.Name(basic.metadata.status)

        def get_facets(message: writer_pb2.BrokerMessage) -> list[str]:
            facets: dict[str, set[str]] = defaultdict(set)

            # Facets from basic
            if message.HasField("basic"):
                basic = message.basic
                if basic.icon:
                    facets["n"].add(f"i/{basic.icon}")
                if not basic.metadata.useful:
                    facets["n"].add(f"s/EMPTY")
                else:
                    status_str = METADATA_STATUS_PB_TYPE_TO_NAME_MAP[basic.metadata.status]
                    facets["n"].add(f"s/{status_str}")
                if basic.metadata.language:
                    facets["s"].add(f"p/{basic.metadata.language}")
                for lang in basic.metadata.languages:
                    facets["s"].add(f"s/{lang}")
                for classification in basic.usermetadata.classifications:
                    if not classification.cancelled_by_user:
                        facets["l"].add(f"{classification.labelset}/{classification.label}")
                if basic.hidden:
                    facets["q"].add("h")

            # Facets from origin
            if message.HasField("origin"):
                origin = message.origin
                if origin.source_id:
                    facets["o"].add(origin.source_id)
                    facets["u"].add(f"s/{origin.source_id}")
                for tag in origin.tags:
                    facets["t"].add(tag)
                if origin.path:
                    facets["p"].add(origin.path.lstrip("/"))
                for contrib in origin.colaborators:
                    facets["u"].add(f"o/{contrib}")
                for key, value in origin.metadata.items():
                    facets["m"].add(f"{key[:255]}/{value[:255]}")

            # Flatten facets
            flat_facets = []
            for key, values in facets.items():
                for value in values:
                    flat_facets.append(f"/{key}/{value}")
            return flat_facets

        created_at, modified_at = get_dates(message)
        content = ResourceCreateContent(
            slug=basic.slug or uuid,
            basic=documents.to_json(basic),
            origin=documents.to_json(message.origin) if message.HasField("origin") else None,
            extra=documents.to_json(message.extra) if message.HasField("extra") else None,
            security=documents.to_json(message.security) if message.HasField("security") else None,
            created_at=created_at,
            modified_at=modified_at,
            processing_status=get_processing_status(basic),
            facets=get_facets(message),
        )
        # TODO(Marklogic): origin relation triplets
        # TODO(Marklogic): user relation triplets
        await resource_create(txn, kbid, uuid, content)

    async def create_fields(
        self,
        txn: MarkLogicTransaction,
        message: writer_pb2.BrokerMessage,
        kbid: str,
        uuid: str,
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

        # Marklogic
        for field, text in message.texts.items():
            field_content = FieldContent(
                id=field,
                type="t",
                value=documents.to_json(text),
                md5=get_text_md5(text),
                status=get_field_status(resources_pb2.FieldType.TEXT, field),
            )
            await fields_create(txn, kbid, uuid, field_content)

        for field, link in message.links.items():
            field_content = FieldContent(
                id=field,
                type="u",
                value=documents.to_json(link),
                md5=None,
                status=get_field_status(resources_pb2.FieldType.LINK, field),
            )
            await fields_create(txn, kbid, uuid, field_content)

        for field, file in message.files.items():
            field_content = FieldContent(
                id=field,
                type="f",
                value=documents.to_json(file),
                md5=file.file.md5 or None,
                status=get_field_status(resources_pb2.FieldType.FILE, field),
            )
            await fields_create(txn, kbid, uuid, field_content)

        for field, kv in message.key_value_fields.items():
            field_content = FieldContent(
                id=field,
                type="k",
                value=documents.to_json(kv),
                md5=None,
                status=get_field_status(resources_pb2.FieldType.KEY_VALUE, field),
            )
            await fields_create(txn, kbid, uuid, field_content)

        for field, conversation in message.conversations.items():
            status = get_field_status(resources_pb2.FieldType.CONVERSATION, field)
            await self.create_conversation(txn, kbid, uuid, field, conversation, status)

        # Object storage
        orm_resource = Resource(
            txn=txn,
            storage=await get_storage(),
            kbid=kbid,
            uuid=uuid,
            disable_vectors=False,
        )
        await orm_resource.apply_field_extracted_data(message)

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
                id=field_id,
                type="c",
                page=page_number,
                value=documents.to_json(resources_pb2.Conversation(messages=page_messages)),
            )
            await conversation_page_create(txn, kbid, uuid, page_content)

        await conversation_page_create(
            txn,
            kbid,
            uuid,
            ConversationPageContent(
                id=field_id,
                type="c",
                page=0,
                value=documents.to_json(splits_metadata),
            ),
        )
        await fields_create(
            txn,
            kbid,
            uuid,
            FieldContent(
                id=field_id,
                type="c",
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

    @processor_observer.wrap({"type": "txn"})
    async def txn(
        self,
        message: writer_pb2.BrokerMessage,
        seqid: int,
        partition: str,
        transaction_check: bool = True,
    ) -> None:
        try:
            kbid, uuid = await self.ensure_kb_and_slug(
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
                resource = await resource_get(txn, kbid, uuid)
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

                if resource is None:
                    await self.create_fields(txn, message, kbid, uuid)
                    await self.create_resource(txn, message, kbid, uuid)
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


class MarkLogicURIs:
    @classmethod
    def kb_registry(cls, kbid: str) -> str:
        return f"/kb/{kbid}/registry.json"

    @classmethod
    def resource_directory(cls, rid: str) -> str:
        return f"/resources/{rid}/"

    @classmethod
    def resource(cls, rid: str) -> str:
        return f"{cls.resource_directory(rid)}data.json"

    @classmethod
    def resource_id_from_uri(cls, uri: object) -> str:
        return str(uri).split("/")[2]

    @classmethod
    def field_directory(cls, rid: str, field_type: str, field_id: str) -> str:
        return f"{cls.resource_directory(rid)}fields/{field_type}/{field_id}/"

    @classmethod
    def field(cls, rid: str, field_type: str, field_id: str) -> str:
        return f"{cls.field_directory(rid, field_type, field_id)}data.json"

    @classmethod
    def conversation_directory(cls, rid: str, field_id: str) -> str:
        return f"{cls.field_directory(rid, 'c', field_id)}pages/"

    @classmethod
    def conversation_page(cls, rid: str, field_id: str, page: int) -> str:
        return f"{cls.conversation_directory(rid, field_id)}{page}.json"


async def kb_exists(
    driver: MarkLogicDriver,
    kbid: str,
) -> bool:
    async with driver.transaction(read_only=True, system=True) as txn:
        content = await documents.read(txn, MarkLogicURIs.kb_registry(kbid))
        return (
            content is not None and content.get("slug") is not None and content.get("deleted_at") is None
        )


async def resource_id_from_slug(driver: MarkLogicDriver, kbid: str, slug: str) -> str | None:
    async with driver.transaction(read_only=True, kbid=kbid) as txn:
        javascript = (
            "cts.uris('', ['document'], cts.andQuery(["
            f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)}), "
            f"cts.pathRangeQuery({json.dumps(MarkLogicIndexPaths.SLUG)}, '=', {json.dumps(slug)})]))"
        )
        uris = await documents.evaluate(txn, javascript)
        return MarkLogicURIs.resource_id_from_uri(uris[0]) if uris else None


async def resource_get(
    txn: MarkLogicTransaction,
    kbid: str,
    uuid: str,
) -> ResourceContent | None:
    content = await documents.read(txn, MarkLogicURIs.resource(uuid))
    return ResourceContent(**content) if content is not None else None


async def resource_delete(
    txn: MarkLogicTransaction,
    kbid: str,
    uuid: str,
) -> None:
    """
    Deletes the resource with the given resource ID (rid) from the database, as well as all related child collections,
    like fields, chunks, embeddings, etc.
    """
    deletion = DocumentDeletion().directory(
        MarkLogicURIs.resource_directory(uuid),
        (
            MarkLogicCollections.FIELDS,
            MarkLogicCollections.CONVERSATIONS,
            MarkLogicCollections.CHUNKS,
            MarkLogicCollections.EMBEDDINGS,
        ),
    )
    await documents.delete_documents(txn, deletion)


async def resource_create(
    txn: MarkLogicTransaction,
    kbid: str,
    uuid: str,
    data: ResourceCreateContent,
) -> None:
    await documents.write(
        txn,
        uri=MarkLogicURIs.resource(uuid),
        collection=MarkLogicCollections.RESOURCES,
        content=data.model_dump(mode="json"),
    )


async def conversation_page_create(
    txn: MarkLogicTransaction,
    kbid: str,
    uuid: str,
    data: ConversationPageContent,
) -> None:
    await documents.write(
        txn,
        uri=MarkLogicURIs.conversation_page(uuid, data.id, data.page),
        collection=MarkLogicCollections.CONVERSATIONS,
        content=data.model_dump(mode="json"),
    )


async def fields_create(
    txn: MarkLogicTransaction,
    kbid: str,
    uuid: str,
    data: FieldContent,
) -> None:
    await documents.write(
        txn,
        uri=MarkLogicURIs.field(uuid, data.type, data.id),
        collection=MarkLogicCollections.FIELDS,
        content=data.model_dump(mode="json"),
    )
