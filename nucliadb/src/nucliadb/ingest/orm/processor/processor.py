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
from typing import Optional

import aiohttp.client_exceptions
import nats.errors
import nats.js.errors
from nidx_protos import noderesources_pb2, nodewriter_pb2
from nidx_protos.noderesources_pb2 import Resource as PBBrainResource

from nucliadb.common import datamanagers, locking
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.external_index_providers.base import ExternalIndexManager
from nucliadb.common.external_index_providers.manager import get_external_index_manager
from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb.common.maindb.exceptions import ConflictError, MaindbServerError
from nucliadb.ingest.orm.exceptions import (
    DeadletteredError,
    InvalidBrokerMessage,
    ResourceNotIndexable,
    SequenceOrderViolation,
)
from nucliadb.ingest.orm.index_message import IndexMessageBuilder
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb.ingest.orm.processor import sequence_manager
from nucliadb.ingest.orm.processor.auditing import collect_audit_fields
from nucliadb.ingest.orm.processor.data_augmentation import (
    get_generated_fields,
    send_generated_fields_to_process,
)
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import (
    knowledgebox_pb2,
    resources_pb2,
    writer_pb2,
)
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage, has_feature

from .pgcatalog import pgcatalog_delete, pgcatalog_update

logger = logging.getLogger("ingest-processor")

MESSAGE_TO_NOTIFICATION_SOURCE = {
    writer_pb2.BrokerMessage.MessageSource.WRITER: writer_pb2.NotificationSource.WRITER,
    writer_pb2.BrokerMessage.MessageSource.PROCESSOR: writer_pb2.NotificationSource.PROCESSOR,
}


def validate_indexable_resource(resource: noderesources_pb2.Resource) -> None:
    """
    It would be more optimal to move this to another layer but it'd also make the code
    more difficult to grok and test because we'd need to move processable check and throw
    an exception in the middle of a bunch of processing logic.

    As it is implemented right now, we just do the check if a resource is indexable right
    before we actually try to index it and not buried it somewhere else in the code base.

    This is still an edge case.
    """
    num_paragraphs = 0
    first_exceeded = ""
    for field_id, fparagraph in resource.paragraphs.items():
        # this count should not be very expensive to do since we don't have
        # a lot of different fields and we just do a count on a dict
        num_paragraphs += len(fparagraph.paragraphs)
        if num_paragraphs > cluster_settings.max_resource_paragraphs:
            first_exceeded = field_id

    if num_paragraphs > cluster_settings.max_resource_paragraphs:
        raise ResourceNotIndexable(
            first_exceeded,
            f"Resource has too many paragraphs ({num_paragraphs}) and cannot be indexed. "
            f"The maximum number of paragraphs per resource is {cluster_settings.max_resource_paragraphs}",
        )


def trim_entity_facets(index_message: PBBrainResource) -> list[tuple[str, str]]:
    max_entities = cluster_settings.max_entity_facets
    warnings = []
    for field_id, text_info in index_message.texts.items():
        if len(text_info.labels) > max_entities:
            new_labels = []
            entity_count = 0
            truncated = False
            for label in text_info.labels:
                if label.startswith("/e/"):
                    if entity_count < max_entities:
                        new_labels.append(label)
                    else:
                        truncated = True
                    entity_count += 1
                else:
                    new_labels.append(label)
            if truncated:
                warnings.append(
                    (
                        field_id,
                        f"Too many detected entities. Only the first {max_entities} will be available as facets for filtering",
                    )
                )
                text_info.ClearField("labels")
                text_info.labels.extend(new_labels)

    return warnings


class Processor:
    """
    This class is responsible for processing messages from the broker
    and attempts to manage sequencing correctly with a txn id implementation.

    The "txn" in this implementation is oriented around the sequence id of
    messages coming through the message broker.

    Not all writes are going to have a transaction id. For example, writes
    coming from processor can be coming through a different channel
    and can not use the txn id
    """

    messages: dict[str, list[writer_pb2.BrokerMessage]]

    def __init__(
        self,
        driver: Driver,
        storage: Storage,
        pubsub: Optional[PubSubDriver] = None,
        partition: Optional[str] = None,
    ):
        self.messages = {}
        self.driver = driver
        self.storage = storage
        self.partition = partition
        self.pubsub = pubsub
        self.index_node_shard_manager = get_shard_manager()

    async def process(
        self,
        message: writer_pb2.BrokerMessage,
        seqid: int,
        partition: Optional[str] = None,
        transaction_check: bool = True,
    ) -> None:
        partition = partition if self.partition is None else self.partition
        if partition is None:
            raise AttributeError("Can't process message from unknown partition")

        # When running in transactional mode, we need to check that
        # that the current message doesn't violate the sequence order for the
        # current partition
        if transaction_check:
            last_seqid = await sequence_manager.get_last_seqid(self.driver, partition)
            if last_seqid is not None and seqid <= last_seqid:
                raise SequenceOrderViolation(last_seqid)

        if message.type == writer_pb2.BrokerMessage.MessageType.DELETE:
            await self.delete_resource(message, seqid, partition, transaction_check)
        elif message.type == writer_pb2.BrokerMessage.MessageType.AUTOCOMMIT:
            await self.txn([message], seqid, partition, transaction_check)
        elif message.type == writer_pb2.BrokerMessage.MessageType.MULTI:
            # XXX Not supported right now
            # MULTI, COMMIT and ROLLBACK are all not supported in transactional mode right now
            # This concept is probably not tenable with current architecture because
            # of how nats works and how we would need to manage rollbacks.
            # XXX Should this be removed?
            await self.multi(message, seqid)
        elif message.type == writer_pb2.BrokerMessage.MessageType.COMMIT:
            await self.commit(message, seqid, partition)
        elif message.type == writer_pb2.BrokerMessage.MessageType.ROLLBACK:
            await self.rollback(message, seqid, partition)

    async def get_resource_uuid(self, kb: KnowledgeBox, message: writer_pb2.BrokerMessage) -> str:
        if message.uuid is None:
            uuid = await kb.get_resource_uuid_by_slug(message.slug)
        else:
            uuid = message.uuid
        return uuid

    @processor_observer.wrap({"type": "delete_resource"})
    async def delete_resource(
        self,
        message: writer_pb2.BrokerMessage,
        seqid: int,
        partition: str,
        transaction_check: bool = True,
    ) -> None:
        async with self.driver.rw_transaction() as txn:
            try:
                kb = KnowledgeBox(txn, self.storage, message.kbid)

                uuid = await self.get_resource_uuid(kb, message)
                async with locking.distributed_lock(
                    locking.RESOURCE_INDEX_LOCK.format(kbid=message.kbid, resource_id=uuid)
                ):
                    # we need to have a lock at indexing time because we don't know if
                    # a resource was in the process of being moved when a delete occurred
                    shard_id = await datamanagers.resources.get_resource_shard_id(
                        txn, kbid=message.kbid, rid=uuid
                    )
                if shard_id is None:
                    logger.warning(f"Resource {uuid} does not exist")
                else:
                    shard = await kb.get_resource_shard(shard_id)
                    if shard is None:
                        raise AttributeError("Shard not available")
                    await pgcatalog_delete(txn, message.kbid, uuid)
                    external_index_manager = await get_external_index_manager(kbid=message.kbid)
                    if external_index_manager is not None:
                        await self.external_index_delete_resource(external_index_manager, uuid)
                    else:
                        await self.index_node_shard_manager.delete_resource(
                            shard, message.uuid, seqid, partition, message.kbid
                        )
                    try:
                        await kb.delete_resource(message.uuid)
                    except Exception as exc:
                        await txn.abort()
                        await self.notify_abort(
                            partition=partition,
                            seqid=seqid,
                            multi=message.multiid,
                            kbid=message.kbid,
                            rid=message.uuid,
                            source=message.source,
                        )
                        raise exc
            finally:
                if txn.open:
                    if transaction_check:
                        await sequence_manager.set_last_seqid(txn, partition, seqid)
                    await txn.commit()
        await self.notify_commit(
            partition=partition,
            seqid=seqid,
            multi=message.multiid,
            message=message,
            write_type=writer_pb2.Notification.WriteType.DELETED,
        )

    @processor_observer.wrap({"type": "commit_slug"})
    async def commit_slug(self, resource: Resource) -> None:
        # Slug may have conflicts as its not partitioned properly,
        # so we commit it in a different transaction to make it as short as possible
        prev_txn = resource.txn
        try:
            async with self.driver.rw_transaction() as txn:
                resource.txn = txn
                await resource.set_slug()
                await txn.commit()
        finally:
            resource.txn = prev_txn

    @processor_observer.wrap({"type": "txn"})
    async def txn(
        self,
        messages: list[writer_pb2.BrokerMessage],
        seqid: int,
        partition: str,
        transaction_check: bool = True,
    ) -> None:
        if len(messages) == 0:
            return None

        kbid = messages[0].kbid
        if not await datamanagers.atomic.kb.exists_kb(kbid=kbid):
            logger.info(f"KB {kbid} is deleted: skiping txn")
            if transaction_check:
                async with datamanagers.with_rw_transaction() as txn:
                    await sequence_manager.set_last_seqid(txn, partition, seqid)
                    await txn.commit()
            return None

        async with self.driver.rw_transaction() as txn:
            try:
                multi = messages[0].multiid
                kb = KnowledgeBox(txn, self.storage, kbid)
                uuid = await self.get_resource_uuid(kb, messages[0])
                resource: Optional[Resource] = None
                handled_exception = None
                created = False

                for message in messages:
                    if resource is not None:
                        assert resource.uuid == message.uuid

                    if message.source == writer_pb2.BrokerMessage.MessageSource.WRITER:
                        resource = await kb.get(uuid)
                        if resource is None:
                            # It's a new resource
                            resource = await kb.add_resource(uuid, message.slug, message.basic)
                            created = True
                        else:
                            # It's an update from writer for an existing resource
                            ...

                    elif message.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
                        resource = await kb.get(uuid)
                        if resource is None:
                            logger.info(
                                f"Secondary message for resource {message.uuid} and resource does not exist, ignoring"
                            )
                            continue
                        else:
                            # It's an update from processor for an existing resource
                            ...

                        generated_fields = await get_generated_fields(message, resource)
                        if generated_fields.is_not_empty():
                            await send_generated_fields_to_process(
                                kbid, resource, generated_fields, message
                            )
                            # TODO: remove this when processor sends the field set
                            for generated_text in generated_fields.texts:
                                message.texts[
                                    generated_text
                                ].generated_by.data_augmentation.SetInParent()

                    else:
                        raise InvalidBrokerMessage(f"Unknown broker message source: {message.source}")

                    # apply changes from the broker message to the resource
                    await self.apply_resource(message, resource, update=(not created))

                # index message
                if resource and resource.modified:
                    index_message = await self.generate_index_message(resource, messages, created)
                    try:
                        warnings = await self.index_resource(
                            index_message=index_message,
                            txn=txn,
                            uuid=uuid,
                            kbid=kbid,
                            seqid=seqid,
                            partition=partition,
                            kb=kb,
                            source=messages_source(messages),
                        )
                        # Save indexing warnings
                        for field_id, warning in warnings:
                            await resource.add_field_error(
                                field_id, warning, writer_pb2.Error.Severity.WARNING
                            )
                    except ResourceNotIndexable as e:
                        await resource.add_field_error(
                            e.field_id, e.message, writer_pb2.Error.Severity.ERROR
                        )
                        # Catalog takes status from index message labels, override it to error
                        current_status = [x for x in index_message.labels if x.startswith("/n/s/")]
                        if current_status:
                            index_message.labels.remove(current_status[0])
                            index_message.labels.append("/n/s/ERROR")

                    await pgcatalog_update(txn, kbid, resource, index_message)

                    if transaction_check:
                        await sequence_manager.set_last_seqid(txn, partition, seqid)
                    await txn.commit()

                    if created:
                        await self.commit_slug(resource)

                    await self.notify_commit(
                        partition=partition,
                        seqid=seqid,
                        multi=multi,
                        message=message,
                        write_type=(
                            writer_pb2.Notification.WriteType.CREATED
                            if created
                            else writer_pb2.Notification.WriteType.MODIFIED
                        ),
                    )
                elif resource and resource.modified is False:
                    await txn.abort()
                    await self.notify_abort(
                        partition=partition,
                        seqid=seqid,
                        multi=multi,
                        kbid=kbid,
                        rid=uuid,
                        source=message.source,
                    )
                    logger.info("This message did not modify the resource")
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
                    multi=multi,
                    kbid=kbid,
                    rid=uuid,
                    source=message.source,
                )
                raise
            except Exception as exc:
                # As we are in the middle of a transaction, we cannot let the exception raise directly
                # as we need to do some cleanup. The exception will be reraised at the end of the function
                # and then handled by the top caller, so errors can be handled in the same place.
                await self.deadletter(messages, partition, seqid)
                await self.notify_abort(
                    partition=partition,
                    seqid=seqid,
                    multi=multi,
                    kbid=kbid,
                    rid=uuid,
                    source=message.source,
                )
                handled_exception = exc
            finally:
                if resource is not None:
                    resource.clean()
                # txn should be already commited or aborted, but in the event of an exception
                # it could be left open. Make sure to close it if it's still open
                if txn.open:
                    await txn.abort()

            if handled_exception is not None:
                if seqid == -1:
                    raise handled_exception
                else:
                    if resource is not None:
                        await self._mark_resource_error(kb, resource, partition, seqid)
                    raise DeadletteredError() from handled_exception

        return None

    async def get_or_assign_resource_shard(
        self, txn: Transaction, kb: KnowledgeBox, uuid: str
    ) -> writer_pb2.ShardObject:
        kbid = kb.kbid
        async with locking.distributed_lock(
            locking.RESOURCE_INDEX_LOCK.format(kbid=kbid, resource_id=uuid)
        ):
            # we need to have a lock at indexing time because we don't know if
            # a resource was move to another shard while it was being indexed
            shard_id = await datamanagers.resources.get_resource_shard_id(txn, kbid=kbid, rid=uuid)

        shard = None
        if shard_id is not None:
            # Resource already has a shard assigned
            shard = await kb.get_resource_shard(shard_id)
            if shard is None:
                raise AttributeError("Shard not available")
        else:
            # It's a new resource, get KB's current active shard to place new resource on
            shard = await self.index_node_shard_manager.get_current_active_shard(txn, kbid)
            if shard is None:
                # No current shard available, create a new one
                shard = await self.index_node_shard_manager.create_shard_by_kbid(txn, kbid)
            await datamanagers.resources.set_resource_shard_id(
                txn, kbid=kbid, rid=uuid, shard=shard.shard
            )
        return shard

    @processor_observer.wrap({"type": "index_resource"})
    async def index_resource(
        self,
        index_message: PBBrainResource,
        txn: Transaction,
        uuid: str,
        kbid: str,
        seqid: int,
        partition: str,
        kb: KnowledgeBox,
        source: nodewriter_pb2.IndexMessageSource.ValueType,
    ) -> list[tuple[str, str]]:
        validate_indexable_resource(index_message)
        warnings = trim_entity_facets(index_message)

        shard = await self.get_or_assign_resource_shard(txn, kb, uuid)
        external_index_manager = await get_external_index_manager(kbid=kbid)
        if external_index_manager is not None:
            await self.external_index_add_resource(external_index_manager, uuid, index_message)
        else:
            await self.index_node_shard_manager.add_resource(
                shard,
                index_message,
                seqid,
                partition=partition,
                kb=kbid,
                source=source,
            )
        return warnings

    @processor_observer.wrap({"type": "generate_index_message"})
    async def generate_index_message(
        self,
        resource: Resource,
        messages: list[writer_pb2.BrokerMessage],
        resource_created: bool,
    ) -> PBBrainResource:
        builder = IndexMessageBuilder(resource)
        message_source = messages_source(messages)
        if message_source == nodewriter_pb2.IndexMessageSource.WRITER:
            return await builder.for_writer_bm(messages, resource_created)
        elif message_source == nodewriter_pb2.IndexMessageSource.PROCESSOR:
            return await builder.for_processor_bm(messages)
        else:  # pragma: no cover
            raise InvalidBrokerMessage(f"Unknown broker message source: {message_source}")

    async def external_index_delete_resource(
        self, external_index_manager: ExternalIndexManager, resource_uuid: str
    ):
        if self.should_skip_external_index(external_index_manager):
            logger.warning(
                "Skipping external index delete resource",
                extra={
                    "kbid": external_index_manager.kbid,
                    "rid": resource_uuid,
                    "provider": external_index_manager.type.value,
                },
            )
            return
        await external_index_manager.delete_resource(resource_uuid=resource_uuid)

    def should_skip_external_index(self, external_index_manager: ExternalIndexManager) -> bool:
        """
        This is a safety measure to skip external indexing in case that the external index provider is not working.
        As we don't want to block the ingestion pipeline, this is a temporary measure until we implement async consumers
        to index to external indexes.
        """
        kbid = external_index_manager.kbid
        provider_type = external_index_manager.type.value
        return has_feature(
            const.Features.SKIP_EXTERNAL_INDEX,
            context={"kbid": kbid, "provider": provider_type},
            default=False,
        )

    async def external_index_add_resource(
        self,
        external_index_manager: ExternalIndexManager,
        resource_uuid: str,
        index_message: PBBrainResource,
    ):
        if not has_vectors_operation(index_message):
            return
        if self.should_skip_external_index(external_index_manager):
            logger.warning(
                "Skipping external index for resource",
                extra={
                    "kbid": external_index_manager.kbid,
                    "rid": resource_uuid,
                    "provider": external_index_manager.type.value,
                },
            )
            return
        await external_index_manager.index_resource(
            resource_uuid=resource_uuid, resource_data=index_message
        )

    async def multi(self, message: writer_pb2.BrokerMessage, seqid: int) -> None:
        self.messages.setdefault(message.multiid, []).append(message)

    async def commit(self, message: writer_pb2.BrokerMessage, seqid: int, partition: str) -> None:
        if message.multiid not in self.messages:
            # Error
            logger.error(f"Closed multi {message.multiid}")
            await self.deadletter([message], partition, seqid)
        else:
            await self.txn(self.messages[message.multiid], seqid, partition)

    async def rollback(self, message: writer_pb2.BrokerMessage, seqid: int, partition: str) -> None:
        # Error
        logger.error(f"Closed multi {message.multiid}")
        del self.messages[message.multiid]
        await self.notify_abort(
            partition=partition,
            seqid=seqid,
            multi=message.multiid,
            kbid=message.kbid,
            rid=message.uuid,
            source=message.source,
        )

    async def deadletter(
        self, messages: list[writer_pb2.BrokerMessage], partition: str, seqid: int
    ) -> None:
        for seq, message in enumerate(messages):
            await self.storage.deadletter(message, seq, seqid, partition)

    @processor_observer.wrap({"type": "apply_resource"})
    async def apply_resource(
        self,
        message: writer_pb2.BrokerMessage,
        resource: Resource,
        update: bool = False,
    ):
        """
        Apply broker message to resource object in the persistence layers (maindb and storage).
        DO NOT add any indexing logic here.
        """
        if update:
            await self.maybe_update_resource_basic(resource, message)

        if message.HasField("origin"):
            await resource.set_origin(message.origin)

        if message.HasField("extra"):
            await resource.set_extra(message.extra)

        if message.HasField("security"):
            await resource.set_security(message.security)

        if message.HasField("user_relations"):
            await resource.set_user_relations(message.user_relations)

        await resource.apply_fields(message)
        await resource.apply_extracted(message)

    async def maybe_update_resource_basic(
        self, resource: Resource, message: writer_pb2.BrokerMessage
    ) -> None:
        basic_field_updates = message.HasField("basic")
        deleted_fields = len(message.delete_fields) > 0
        if not (basic_field_updates or deleted_fields):
            return

        await resource.set_basic(
            message.basic,
            deleted_fields=message.delete_fields,  # type: ignore
        )

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
        multi: str,
        message: writer_pb2.BrokerMessage,
        write_type: writer_pb2.Notification.WriteType.ValueType,
    ):
        message_audit = await self.get_extended_audit_data(message)
        notification = writer_pb2.Notification(
            partition=int(partition),
            seqid=seqid,
            multi=multi,
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
        multi: str,
        kbid: str,
        rid: str,
        source: writer_pb2.BrokerMessage.MessageSource.ValueType,
    ):
        message = writer_pb2.Notification(
            partition=int(partition),
            seqid=seqid,
            multi=multi,
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

    async def _mark_resource_error(
        self, kb: KnowledgeBox, resource: Optional[Resource], partition: str, seqid: int
    ) -> None:
        """
        Unhandled error processing, try to mark resource as error
        """
        if resource is None or resource.basic is None:
            logger.info(f"Skip when resource does not even have basic metadata: {resource}")
            return
        try:
            async with self.driver.rw_transaction() as txn:
                kb.txn = resource.txn = txn
                resource.basic.metadata.status = resources_pb2.Metadata.Status.ERROR
                await resource.set_basic(resource.basic)
                await txn.commit()
        except Exception:
            logger.warning("Error while marking resource as error", exc_info=True)

    # KB tools
    # XXX: Why are these utility functions here?
    async def get_kb_obj(
        self, txn: Transaction, kbid: knowledgebox_pb2.KnowledgeBoxID
    ) -> Optional[KnowledgeBox]:
        uuid: Optional[str] = kbid.uuid
        if uuid == "":
            uuid = await datamanagers.kb.get_kb_uuid(txn, slug=kbid.slug)

        if uuid is None:
            return None

        if not (await datamanagers.kb.exists_kb(txn, kbid=uuid)):
            return None

        storage = await get_storage()
        kbobj = KnowledgeBox(txn, storage, uuid)
        return kbobj


def messages_source(messages: list[writer_pb2.BrokerMessage]):
    from_writer = all(
        (message.source == writer_pb2.BrokerMessage.MessageSource.WRITER for message in messages)
    )
    from_processor = all(
        (message.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR for message in messages)
    )
    if from_writer:
        source = nodewriter_pb2.IndexMessageSource.WRITER
    elif from_processor:
        source = nodewriter_pb2.IndexMessageSource.PROCESSOR
    else:  # pragma: no cover
        msg = "Processor received multiple broker messages with different sources in the same txn!"
        logger.error(msg)
        errors.capture_exception(Exception(msg))
        source = nodewriter_pb2.IndexMessageSource.PROCESSOR
    return source


def has_vectors_operation(index_message: PBBrainResource) -> bool:
    """
    Returns True if the index message has any vectors to index or to delete.
    """
    if any([len(deletions.items) for deletions in index_message.vector_prefixes_to_delete.values()]):
        return True
    for field_paragraphs in index_message.paragraphs.values():
        for paragraph in field_paragraphs.paragraphs.values():
            if len(paragraph.sentences) > 0:
                return True
            for vectorset_sentences in paragraph.vectorsets_sentences.values():
                if len(vectorset_sentences.sentences) > 0:
                    return True
    return False


def needs_reindex(bm: writer_pb2.BrokerMessage) -> bool:
    return bm.reindex or is_vectorset_migration_bm(bm)


def is_vectorset_migration_bm(bm: writer_pb2.BrokerMessage) -> bool:
    """
    This is a temporary solution to avoid duplicating paragraphs and text fields during vector migrations.
    We need to reindex all the fields of a resource to avoid this issue.
    TODO: Remove this when the index message generation logic has been decoupled into its own method.

    Broker messages from semantic model migration task only contain the `field_vectors` field set.
    """
    return (
        len(bm.field_vectors) > 0
        and not bm.HasField("basic")
        and len(bm.delete_fields) == 0
        and len(bm.files) == 0
        and len(bm.texts) == 0
        and len(bm.conversations) == 0
        and len(bm.links) == 0
    )
