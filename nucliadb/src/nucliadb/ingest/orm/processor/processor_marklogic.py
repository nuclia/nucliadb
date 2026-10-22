# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
import asyncio
import logging
from typing import Any, Literal, cast

import aiohttp.client_exceptions
import nats.errors
import nats.js.errors
from nidx_protos import noderesources_pb2 as brain

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.resources import ResourceCreateContent, ResourceUpdateContent
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.exceptions import ConflictError, MaindbServerError
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb.common.marklogic import documents, graph
from nucliadb.common.marklogic.document_update import DocumentUpdate
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.exceptions import (
    DeadletteredError,
    InvalidBrokerMessage,
    SequenceOrderViolation,
)
from nucliadb.ingest.orm.index_message import FieldData, FieldValue, IndexMessageBuilder, ResourceData
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb.ingest.orm.processor import sequence_manager
from nucliadb.ingest.orm.processor.auditing import collect_audit_fields
from nucliadb.ingest.orm.processor.data_augmentation import (
    get_generated_fields,
    send_generated_fields_to_process_v2,
)
from nucliadb.ingest.orm.processor.processor import (
    MESSAGE_TO_NOTIFICATION_SOURCE,
    _apply_extracted_basic_updates,
    _merge_basic,
    compute_resource_status,
)
from nucliadb.ingest.orm.resource import (
    MAX_FIELD_STATUS_ERRORS,
    Resource,
)
from nucliadb_models.labels import LABEL_HIDDEN
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_protos import (
    knowledgebox_pb2,
    resources_pb2,
    utils_pb2,
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
        if uuid:
            if not await datamanagers.atomic.kb.exists(kbid=uuid):
                return None
        else:
            uuid = await datamanagers.kb.get_kbid(txn, slug=kbid.slug)
        if not uuid:
            return None
        storage = await get_storage()
        kbobj = KnowledgeBox(txn, storage, uuid)
        return kbobj

    async def get_extended_audit_data(self, bm: writer_pb2.BrokerMessage) -> writer_pb2.Audit:
        audit_pb = writer_pb2.Audit()
        audit_pb.CopyFrom(bm.audit)
        audit_pb.kbid = bm.kbid
        audit_pb.uuid = bm.uuid
        audit_pb.message_source = bm.source
        audit_pb.field_metadata.extend([fcmw.field for fcmw in bm.field_metadata])
        audit_fields = await collect_audit_fields(self.driver, self.storage, bm)
        audit_pb.audit_fields.extend(audit_fields)
        return audit_pb

    async def notify_commit(
        self,
        *,
        partition: str,
        seqid: int,
        bm: writer_pb2.BrokerMessage,
        write_type: writer_pb2.Notification.WriteType.ValueType,
    ):
        audit_pb = await self.get_extended_audit_data(bm)
        notification = writer_pb2.Notification(
            partition=int(partition),
            seqid=seqid,
            multi="",
            uuid=bm.uuid,
            kbid=bm.kbid,
            action=writer_pb2.Notification.Action.COMMIT,
            write_type=write_type,
            source=MESSAGE_TO_NOTIFICATION_SOURCE[bm.source],
            processing_errors=len(bm.errors) > 0,
            message_audit=audit_pb,
        )

        await self.notify(
            const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=bm.kbid),
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
        bm = writer_pb2.Notification(
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
            bm.SerializeToString(),
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
        bm = message
        partition = partition if self.partition is None else self.partition
        if partition is None:
            raise RuntimeError("Can't process bm from unknown partition")

        if transaction_check:
            last_seqid = await sequence_manager.get_last_seqid(self.driver, partition)
            if last_seqid is not None and seqid <= last_seqid:
                raise SequenceOrderViolation(last_seqid)

        if bm.type == writer_pb2.BrokerMessage.MessageType.DELETE:
            await self.delete(bm, seqid, partition, transaction_check)
        elif bm.type == writer_pb2.BrokerMessage.MessageType.AUTOCOMMIT:
            await self.txn(bm, seqid, partition, transaction_check)
        else:  # pragma: no cover
            logger.error(
                f"Unsupported bm type: {bm.type}",
                extra={"seqid": seqid, "partition": partition},
            )

    @processor_observer.wrap({"type": "delete_resource"})
    async def delete(
        self,
        bm: writer_pb2.BrokerMessage,
        seqid: int,
        partition: str,
        transaction_check: bool = True,
    ) -> None:
        try:
            kbid, uuid = await self.ensure_kb_and_resource(bm.kbid, bm.uuid, bm.slug)
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
                    source=bm.source,
                )
                raise exc
        await self.notify_commit(
            partition=partition,
            seqid=seqid,
            bm=bm,
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

    async def deadletter(self, bm: writer_pb2.BrokerMessage, partition: str, seqid: int) -> None:
        await self.storage.deadletter(bm, 0, seqid, partition)

    @processor_observer.wrap({"type": "handle_data_augmentation_fields"})
    async def handle_data_augmentation_fields(self, bm: writer_pb2.BrokerMessage, kbid: str, rid: str):
        """
        Fields generated by data augmentation tasks are sent back to the processor to be processed and indexed.
        """
        generated_fields = await get_generated_fields(bm)
        if generated_fields.is_not_empty():
            await send_generated_fields_to_process_v2(kbid, rid, generated_fields, bm)

    def writer_resource_content(
        self,
        bm: writer_pb2.BrokerMessage,
        uuid: str,
    ) -> ResourceCreateContent:
        assert bm.HasField("basic")
        basic = bm.basic

        # Make sure the slug is set in both the broker message and the basic message
        slug = bm.slug or basic.slug or uuid
        bm.slug = basic.slug = slug

        origin = bm.origin if bm.HasField("origin") else None
        created_at = (
            origin.created.ToJsonString()
            if origin is not None and origin.HasField("created")
            else basic.created.ToJsonString()
            if basic.HasField("created")
            else ""
        )
        modified_at = (
            origin.modified.ToJsonString()
            if origin is not None and origin.HasField("modified")
            else basic.modified.ToJsonString()
            if basic.HasField("modified")
            else ""
        )
        return ResourceCreateContent(
            slug=slug,
            basic=documents.to_json(basic),
            origin=documents.to_json(bm.origin) if bm.HasField("origin") else None,
            extra=documents.to_json(bm.extra) if bm.HasField("extra") else None,
            security=documents.to_json(bm.security) if bm.HasField("security") else None,
            title=basic.title or slug,
            created_at=created_at,
            modified_at=modified_at,
            processing_status=writer_pb2.Metadata.Status.Name(basic.metadata.status),
            facets=sorted(
                {
                    f"/l/{classification.labelset}/{classification.label}"
                    for classification in basic.usermetadata.classifications
                    if not classification.cancelled_by_user
                }
                | ({LABEL_HIDDEN} if basic.hidden else set())
            ),
        )

    @processor_observer.wrap({"type": "apply_resource"})
    async def create_resource(
        self,
        txn: MarkLogicTransaction,
        bm: writer_pb2.BrokerMessage,
        kbid: str,
        uuid: str,
    ) -> None:
        content = self.writer_resource_content(bm, uuid)
        with processor_observer({"type": "commit_slug"}):
            await datamanagers.resources.create_resource(txn, kbid=kbid, rid=uuid, data=content)

        # User relations are stored in the object store
        if bm.HasField("user_relations"):
            await self.orm_resource(txn, kbid, uuid).set_user_relations(bm.user_relations)

    def orm_resource(self, txn: MarkLogicTransaction, kbid: str, uuid: str) -> Resource:
        return Resource(txn=txn, storage=self.storage, kbid=kbid, uuid=uuid, disable_vectors=False)

    async def get_updated_status(
        self,
        resource: Resource,
        bm: writer_pb2.BrokerMessage,
        field_id: resources_pb2.FieldID,
    ) -> writer_pb2.FieldStatus:
        from_processor = any(
            generator.WhichOneof("generator") == "processor" for generator in bm.generated_by
        )
        status = writer_pb2.FieldStatus()
        if not from_processor:
            field = await resource.get_field(field_id.field, field_id.field_type, load=False)
            previous = await field.get_status()
            if previous is not None:
                status.CopyFrom(previous)
        for error in bm.errors:
            if error.field_type == field_id.field_type and error.field == field_id.field:
                field_error = status.errors.add(source_error=error)
                field_error.created.GetCurrentTime()
        if len(status.errors) > MAX_FIELD_STATUS_ERRORS:
            errors_to_trim = len(status.errors) - MAX_FIELD_STATUS_ERRORS
            logger.warning(
                "Field status error limit exceeded, trimming tail errors",
                extra={
                    "kbid": resource.kbid,
                    "rid": resource.uuid,
                    "field_type": field_id.field_type,
                    "field": field_id.field,
                    "stored_errors": len(status.errors),
                    "max_errors": MAX_FIELD_STATUS_ERRORS,
                },
            )
            del status.errors[-errors_to_trim:]
        if bm.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
            status.status = (
                writer_pb2.FieldStatus.Status.ERROR
                if any(
                    error.source_error.severity == writer_pb2.Error.Severity.ERROR
                    for error in status.errors
                )
                else writer_pb2.FieldStatus.Status.PROCESSED
            )
        else:
            explicit = next((entry for entry in bm.field_statuses if entry.id == field_id), None)
            if explicit is not None:
                status.status = explicit.status
        return status

    @processor_observer.wrap({"type": "apply_fields"})
    @processor_observer.wrap({"type": "apply_field_values"})
    async def create_fields(
        self,
        txn: MarkLogicTransaction,
        bm: writer_pb2.BrokerMessage,
        kbid: str,
        uuid: str,
    ) -> None:
        orm_resource = self.orm_resource(txn, kbid, uuid)
        for field_type, values in (
            (resources_pb2.FieldType.TEXT, bm.texts),
            (resources_pb2.FieldType.LINK, bm.links),
            (resources_pb2.FieldType.FILE, bm.files),
            (resources_pb2.FieldType.KEY_VALUE, bm.key_value_fields),
            (resources_pb2.FieldType.CONVERSATION, bm.conversations),
        ):
            for field_name, payload in values.items():
                field_id = resources_pb2.FieldID(field_type=field_type, field=field_name)
                status = await self.get_updated_status(orm_resource, bm, field_id)
                field = await orm_resource.get_field(field_name, field_type, load=False)
                await field.set_value(payload, status=status)

    async def persist_fields_processed_data(
        self, txn, kbid: str, uuid: str, bm: writer_pb2.BrokerMessage, is_new_resource: bool = False
    ):
        # Save extracted data from the broker message to the object store
        resource = self.orm_resource(txn, kbid, uuid)
        with processor_observer({"type": "apply_fields"}):
            with processor_observer({"type": "apply_field_extracted_data"}):
                await resource.apply_field_extracted_data(bm)

        await self.apply_extracted_basic_updates(resource, bm)

        # Now store all the processed contents to Marklogic (embeddings, chunks, etc)
        im = await self.generate_index_message(txn, uuid, bm, is_new_resource=is_new_resource)
        await self.index_resource(txn, kbid, uuid, im, is_new_resource)

    @processor_observer.wrap({"type": "index_resource"})
    async def index_resource(
        self,
        txn: MarkLogicTransaction,
        kbid: str,
        uuid: str,
        im: brain.Resource,
        is_new_resource: bool,
    ) -> None:
        for fpc, chunks, embeddings in await get_field_processed_contents(txn, kbid, im):
            if not is_new_resource:
                full_field_id = f"{uuid}/{fpc.field_type}/{fpc.field_id}"
                # Only replaced fields: conversation appends must keep their previous chunks/embeddings
                replaced_models = [
                    vectorset
                    for vectorset, prefixes in im.vector_prefixes_to_delete.items()
                    if full_field_id in prefixes.items
                ]
                await datamanagers.fields.delete_processed_documents(
                    txn,
                    kbid=kbid,
                    rid=uuid,
                    field_type=fpc.field_type,
                    field_id=fpc.field_id,
                    field_texts=not im.skip_texts,
                    chunks=(
                        len(chunks) > 0
                        and not im.skip_paragraphs
                        and full_field_id in im.paragraphs_to_delete
                    ),
                    embeddings=len(embeddings) > 0,
                    embedding_models=replaced_models,
                )
            await datamanagers.fields.create_processed_documents(
                txn=txn,
                field=fpc,
                chunks=chunks,
                embeddings=embeddings,
                skip_field_texts=False,
                skip_chunks=im.skip_paragraphs,
            )

    @processor_observer.wrap({"type": "apply_resource"})
    async def apply_extracted_basic_updates(
        self, orm_resource: Resource, bm: writer_pb2.BrokerMessage
    ) -> None:
        basic = await orm_resource.get_basic()
        previous_basic = writer_pb2.Basic()
        previous_basic.CopyFrom(basic)
        if bm.link_extracted_data or bm.file_extracted_data or bm.field_metadata:
            filenames = await orm_resource.get_filenames()
            _apply_extracted_basic_updates(bm, basic, orm_resource.uuid, filenames)
        # Field statuses are already persisted, fold them into the resource status
        await compute_resource_status(orm_resource.txn, orm_resource.kbid, orm_resource.uuid, basic)
        if basic != previous_basic:
            await datamanagers.resources.update_resource(
                orm_resource.txn,
                kbid=orm_resource.kbid,
                rid=orm_resource.uuid,
                data=ResourceUpdateContent(
                    basic=documents.to_json(basic),
                    title=basic.title,
                    processing_status=writer_pb2.Metadata.Status.Name(basic.metadata.status),
                ),
            )

    @processor_observer.wrap({"type": "apply_resource"})
    async def update_resource(
        self,
        txn: MarkLogicTransaction,
        bm: writer_pb2.BrokerMessage,
        kbid: str,
        uuid: str,
    ) -> None:
        values: dict[str, Any] = {}
        for behavior in ("extra", "security", "origin"):
            if bm.HasField(behavior):
                values[behavior] = documents.to_json(getattr(bm, behavior))
        if bm.HasField("basic") or len(bm.delete_fields) > 0:
            # Merge onto the stored basic so data not sent by the writer (e.g. computed metadata) is kept
            current_basic = await self.orm_resource(txn, kbid, uuid).get_basic()
            payload = writer_pb2.Basic()
            payload.CopyFrom(bm.basic)
            basic = _merge_basic(current_basic, payload, list(bm.delete_fields))
            values.update(
                basic=documents.to_json(basic),
                title=basic.title,
                processing_status=writer_pb2.Metadata.Status.Name(basic.metadata.status),
                facets=sorted(
                    {
                        f"/l/{classification.labelset}/{classification.label}"
                        for classification in basic.usermetadata.classifications
                        if not classification.cancelled_by_user
                    }
                    | ({LABEL_HIDDEN} if basic.hidden else set())
                ),
            )
        timestamps: tuple[tuple[Literal["created", "modified"], str], ...] = (
            ("created", "created_at"),
            ("modified", "modified_at"),
        )
        for timestamp, column in timestamps:
            if bm.HasField("origin") and bm.origin.HasField(timestamp):
                values[column] = getattr(bm.origin, timestamp).ToJsonString()
            elif bm.HasField("basic") and bm.basic.HasField(timestamp):
                values[column] = getattr(bm.basic, timestamp).ToJsonString()
        slug = bm.basic.slug or bm.slug
        if slug:
            values["slug"] = slug
        if values:
            await datamanagers.resources.update_resource(
                txn, kbid=kbid, rid=uuid, data=ResourceUpdateContent(**values)
            )
        if bm.HasField("user_relations"):
            orm_resource = self.orm_resource(txn, kbid, uuid)
            await orm_resource.set_user_relations(bm.user_relations)

    async def update_fields(
        self,
        txn: MarkLogicTransaction,
        bm: writer_pb2.BrokerMessage,
        kbid: str,
        uuid: str,
    ) -> None:
        orm_resource = self.orm_resource(txn, kbid, uuid)

        # Field deletions
        for field in bm.delete_fields:
            field = await orm_resource.get_field(field.field, field.field_type, load=False)
            await field.delete()

        # Message deletions
        for deleted in bm.delete_splits:
            if deleted.field.field_type == writer_pb2.FieldType.CONVERSATION:
                field = await orm_resource.get_field(
                    deleted.field.field, deleted.field.field_type, load=False
                )
                conversation = cast(Conversation, field)
                await conversation.delete_messages(deleted.splits)

        # Set the new field values
        await self.create_fields(txn, bm, kbid, uuid)

    @processor_observer.wrap({"type": "generate_index_message"})
    async def generate_index_message(
        self,
        txn: MarkLogicTransaction,
        uuid: str,
        bm: writer_pb2.BrokerMessage,
        is_new_resource: bool,
    ) -> brain.Resource:
        orm_resource = self.orm_resource(txn, bm.kbid, uuid)
        builder = IndexMessageBuilderV2(orm_resource)
        if bm.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
            return await builder.for_processor_bm(bm)
        elif bm.source == writer_pb2.BrokerMessage.MessageSource.WRITER:
            message = await builder.for_writer_bm(bm, resource_created=is_new_resource)
            for name in ("title", "summary"):
                value = getattr(bm.basic, name)
                if value:
                    field_key = f"a/{name}"
                    message.texts[field_key].text = value
                    paragraph_id = f"{uuid}/{field_key}/0-{len(value)}"
                    paragraph = message.paragraphs[field_key].paragraphs[paragraph_id]
                    paragraph.start = paragraph.metadata.position.start = 0
                    paragraph.end = paragraph.metadata.position.end = len(value)
                    message.paragraphs_to_delete.append(f"{uuid}/{field_key}")
                    message.skip_texts = message.skip_paragraphs = False
            return message
        else:
            raise ValueError(f"Unknown message source: {bm.source}")

    @processor_observer.wrap({"type": "txn"})
    async def txn(
        self,
        bm: writer_pb2.BrokerMessage,
        seqid: int,
        partition: str,
        transaction_check: bool = True,
    ) -> None:
        try:
            kbid, uuid = await self.ensure_kb_and_resource(
                kbid=bm.kbid,
                uuid=bm.uuid,
                slug=bm.slug,
            )
        except (KnowledgeBoxDoesNotExist, ResourceDoesNotExist):
            if transaction_check:
                await self.set_last_seqid(partition, seqid)
            return None

        async with self.driver.transaction(read_only=False, kbid=kbid) as txn:
            handled_exception = None
            # None until the resource lookup succeeds
            is_new_resource: bool | None = None
            try:
                resource = await datamanagers.resources.get_resource(txn, kbid=kbid, rid=uuid)
                is_new_resource = resource is None
                if bm.source == writer_pb2.BrokerMessage.MessageSource.WRITER:
                    if is_new_resource:
                        await self.create_resource(txn, bm, kbid, uuid)
                        await self.create_fields(txn, bm, kbid, uuid)
                    else:
                        await self.update_fields(txn, bm, kbid, uuid)
                        await self.update_resource(txn, bm, kbid, uuid)

                    # Title fields are not processed and are persisted right at creation time
                    await self.persist_fields_processed_data(
                        txn, kbid, uuid, bm, is_new_resource=is_new_resource
                    )

                elif bm.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
                    if is_new_resource:
                        logger.info(
                            f"Processor bm for resource received but the resource does not exist, ignoring.",
                            extra={
                                "kbid": kbid,
                                "rid": uuid,
                                "seqid": seqid,
                            },
                        )
                        if transaction_check:
                            await self.set_last_seqid(partition, seqid)
                        return None
                    await self.update_fields(txn, bm, kbid, uuid)
                    await self.persist_fields_processed_data(txn, kbid, uuid, bm)
                    await self.handle_data_augmentation_fields(bm, kbid, uuid)

                else:
                    raise InvalidBrokerMessage(f"Unhandled broker bm source: {bm.source}")

                if transaction_check:
                    await self.set_last_seqid(partition, seqid)

                await txn.commit()
                await self.notify_commit(
                    partition=partition,
                    seqid=seqid,
                    bm=bm,
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
                    source=bm.source,
                )
                raise
            except Exception as exc:
                # As we are in the middle of a transaction, we cannot let the exception raise directly
                # as we need to do some cleanup. The exception will be reraised at the end of the function
                # and then handled by the top caller, so errors can be handled in the same place.
                logger.error(
                    "Error processing bm. Broker bm stored in dead letter bucket.",
                    exc_info=True,
                    extra={"kbid": kbid, "rid": uuid, "seqid": seqid, "partition": partition},
                )
                await self.deadletter(bm, partition, seqid)
                await self.notify_abort(
                    partition=partition,
                    seqid=seqid,
                    kbid=kbid,
                    rid=uuid,
                    source=bm.source,
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
                    if is_new_resource is False:
                        await self._mark_resource_error(kbid, uuid)
                    raise DeadletteredError() from handled_exception

            return None


class KnowledgeBoxDoesNotExist(Exception):
    """Exception raised when a Knowledge Box does not exist."""


class ResourceDoesNotExist(Exception):
    """Exception raised when a Resource does not exist."""


async def get_field_processed_contents(
    txn: MarkLogicTransaction, kbid: str, im: brain.Resource
) -> list[tuple[FieldProcessedContent, list[ChunkContent], list[ChunkEmbeddingContent]]]:
    """Processed documents of the index message fields, with the copies of their resource properties."""
    resource_id = im.resource.uuid
    resource_copies = await datamanagers.resources.get_processed_field_copies(
        txn, kbid=kbid, rid=resource_id
    )
    contents = []
    if "a/metadata" in im.field_relations or "a/metadata" in im.relation_fields_to_delete:
        resource_relations = [
            graph.relation_content(relation, "", index)
            for index, relation in enumerate(im.field_relations["a/metadata"].relations)
        ]
        await documents.update_document(
            txn,
            DocumentUpdate(
                uri=MarkLogicURIs.resource(resource_id), collection=MarkLogicCollections.RESOURCES
            ).set(relations=resource_relations),
        )
        await documents.delete(txn, MarkLogicURIs.field_processed(resource_id, "metadata", "a"))
    field_ids = (
        set(im.texts)
        | set(im.field_relations)
        | set(im.field_node_vectors)
        | set(im.field_edge_vectors)
        | set(im.relation_fields_to_delete)
    )
    for fid in sorted(field_ids):
        if fid == "a/metadata":
            continue
        field_type, field_id = fid.split("/")
        uri = MarkLogicURIs.field_processed(resource_id, field_id, field_type)
        previous = await documents.read(txn, uri)
        field = FieldProcessedContent.model_validate(
            previous
            or dict(
                resource_id=resource_id,
                field_id=field_id,
                field_type=field_type,
                extracted_text="",
                facets=[],
            )
        )
        for name, value in resource_copies.items():
            setattr(field, name, value)
        if fid in im.texts and not im.skip_texts:
            field.extracted_text = im.texts[fid].text
            field.facets = list(im.texts[fid].labels)
        if fid in im.relation_fields_to_delete:
            field.relations = []
        occurrence_start = len(field.relations)
        field.relations.extend(
            graph.relation_content(relation, f"{resource_id}/{fid}", occurrence_start + index)
            for index, relation in enumerate(im.field_relations[fid].relations)
        )
        full_field_id = f"{resource_id}/{fid}"
        replaced_models = {
            model
            for model, prefixes in im.vector_prefixes_to_delete.items()
            if full_field_id in prefixes.items
        }
        if full_field_id in im.vectors_to_delete_in_all_vectorsets:
            field.graph_vectors = []
        else:
            field.graph_vectors = [
                entry for entry in field.graph_vectors if entry["model_id"] not in replaced_models
            ]
        for kind, models, value_key in (
            ("node", im.field_node_vectors[fid].node_vectors, "node_value"),
            ("edge", im.field_edge_vectors[fid].edge_vectors, "relation_label"),
        ):
            for model, vectors in models.items():
                field.graph_vectors.extend(
                    dict(
                        kind=kind,
                        model_id=model,
                        value=getattr(vector, value_key),
                        normalized=graph.normalize(getattr(vector, value_key)),
                        vector=list(vector.vector),
                    )
                    for vector in vectors.vectors
                )
        if fid not in im.texts and previous is None and not field.relations and not field.graph_vectors:
            continue
        field_text = field.extracted_text
        chunks = []
        embeddings = []
        field_paragraphs = im.paragraphs.get(fid) or brain.IndexParagraphs()
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
        contents.append((field, chunks, embeddings))
    return contents


class IndexMessageBuilderV2(IndexMessageBuilder):
    @property
    def txn(self) -> MarkLogicTransaction:
        return self.resource.txn

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

    async def get_basic(self) -> writer_pb2.Basic:
        return (await self.get_resource_data()).basic

    async def get_resource_fields(self) -> list[tuple[writer_pb2.FieldType.ValueType, str]]:
        fields = await datamanagers.fields.list_resource_documents(
            self.txn, resource_id=self.resource_id
        )
        basic = await self.get_basic()
        fields.extend(
            (writer_pb2.FieldType.GENERIC, name) for name in ("title", "summary") if getattr(basic, name)
        )
        return fields

    async def get_field_data(self, field_id: writer_pb2.FieldID) -> FieldData:
        if field_id.field_type == writer_pb2.FieldType.GENERIC:
            value = getattr(await self.get_basic(), field_id.field)
            metadata = writer_pb2.FieldComputedMetadata()
            if value:
                metadata.metadata.paragraphs.add(start=0, end=len(value))
            return FieldData(
                extracted_text=utils_pb2.ExtractedText(text=value), field_computed_metadata=metadata
            )
        field = await self.resource.get_field(field_id.field, field_id.field_type, load=False)
        et = await field.get_extracted_text()
        fm = await field.get_field_metadata()
        value = await self.get_field_value(field_id)
        generated_by = None
        if isinstance(value, (writer_pb2.FieldConversation, writer_pb2.FieldText)):
            generated_by = value.generated_by
        return FieldData(
            extracted_text=et,
            field_computed_metadata=fm,
            generated_by=generated_by,
        )

    async def get_field_value(self, fieldid: writer_pb2.FieldID) -> FieldValue | None:
        content = await datamanagers.fields.get_document(
            self.txn,
            resource_id=self.resource_id,
            field_id=fieldid.field,
            field_type=from_proto.field_type_name(fieldid.field_type).abbreviation(),
        )
        if content is None or content.value is None:
            return None
        pb_class = {
            writer_pb2.FieldType.TEXT: writer_pb2.FieldText,
            writer_pb2.FieldType.FILE: writer_pb2.FieldFile,
            writer_pb2.FieldType.LINK: writer_pb2.FieldLink,
            writer_pb2.FieldType.KEY_VALUE: writer_pb2.FieldKeyValue,
            writer_pb2.FieldType.CONVERSATION: writer_pb2.FieldConversation,
        }[fieldid.field_type]
        return documents.from_json(content.value, pb_class)
