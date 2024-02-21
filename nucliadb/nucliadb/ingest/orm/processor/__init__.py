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

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.datamanagers.kb import KnowledgeBoxDataManager
from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb.ingest.orm.exceptions import (
    DeadletteredError,
    KnowledgeBoxConflict,
    ResourceNotIndexable,
    SequenceOrderViolation,
)
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb.ingest.orm.processor import sequence_manager
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import (
    knowledgebox_pb2,
    noderesources_pb2,
    nodewriter_pb2,
    resources_pb2,
    utils_pb2,
    writer_pb2,
)
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage

logger = logging.getLogger(__name__)


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
    for _, fparagraph in resource.paragraphs.items():
        # this count should not be very expensive to do since we don't have
        # a lot of different fields and we just do a count on a dict
        num_paragraphs += len(fparagraph.paragraphs)

    if num_paragraphs > cluster_settings.max_resource_paragraphs:
        raise ResourceNotIndexable(
            "Resource has too many paragraphs. "
            f"Supported: {cluster_settings.max_resource_paragraphs} , Number: {num_paragraphs}"
        )


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
        self.shard_manager = get_shard_manager()
        self.kb_data_manager = KnowledgeBoxDataManager(driver)

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

    async def get_resource_uuid(
        self, kb: KnowledgeBox, message: writer_pb2.BrokerMessage
    ) -> str:
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
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, message.kbid)

        uuid = await self.get_resource_uuid(kb, message)
        shard_id = await kb.get_resource_shard_id(uuid)
        if shard_id is None:
            logger.warning(f"Resource {uuid} does not exist")
        else:
            shard = await kb.get_resource_shard(shard_id)
            if shard is None:
                raise AttributeError("Shard not available")

            await self.shard_manager.delete_resource(
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
            async with self.driver.transaction() as txn:
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

        txn = await self.driver.begin()
        kbid = messages[0].kbid
        if not await KnowledgeBox.exist_kb(txn, kbid):
            logger.warning(f"KB {kbid} is deleted: skiping txn")
            if transaction_check:
                await sequence_manager.set_last_seqid(txn, partition, seqid)
            await txn.commit()
            return None

        multi = messages[0].multiid
        kb = KnowledgeBox(txn, self.storage, kbid)
        uuid = await self.get_resource_uuid(kb, messages[0])
        resource: Optional[Resource] = None
        handled_exception = None
        created = False

        try:
            for message in messages:
                if resource is not None:
                    assert resource.uuid == message.uuid
                result = await self.apply_resource(message, kb, resource)

                if result is None:
                    continue

                resource, _created = result
                created = created or _created

            if resource:
                await resource.compute_global_text()
                await resource.compute_global_tags(resource.indexer)
                await resource.compute_security(resource.indexer)
                if message.reindex:
                    # when reindexing, let's just generate full new index message
                    resource.replace_indexer(await resource.generate_index_message())

            if resource and resource.modified:
                await self.index_resource(  # noqa
                    resource=resource,
                    txn=txn,
                    uuid=uuid,
                    kbid=kbid,
                    seqid=seqid,
                    partition=partition,
                    kb=kb,
                    source=messages_source(messages),
                )

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
                    write_type=writer_pb2.Notification.WriteType.CREATED
                    if created
                    else writer_pb2.Notification.WriteType.MODIFIED,
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
                logger.warning("This message did not modify the resource")
        except (
            asyncio.TimeoutError,
            asyncio.CancelledError,
            aiohttp.client_exceptions.ClientError,
            ConflictError,
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

    @processor_observer.wrap({"type": "index_resource"})
    async def index_resource(
        self,
        resource: Resource,
        txn: Transaction,
        uuid: str,
        kbid: str,
        seqid: int,
        partition: str,
        kb: KnowledgeBox,
        source: nodewriter_pb2.IndexMessageSource.ValueType,
    ) -> None:
        validate_indexable_resource(resource.indexer.brain)

        shard_id = await kb.get_resource_shard_id(uuid)

        shard = None
        if shard_id is not None:
            shard = await kb.get_resource_shard(shard_id)

        if shard is None:
            # It's a new resource, get current active shard to place
            # new resource on
            shard = await self.shard_manager.get_current_active_shard(txn, kbid)
            if shard is None:
                # no shard available, create a new one
                model = await self.kb_data_manager.get_model_metadata(kbid)
                config = await kb.get_config()
                if config is not None:
                    release_channel = config.release_channel
                else:
                    release_channel = utils_pb2.ReleaseChannel.STABLE

                shard = await self.shard_manager.create_shard_by_kbid(
                    txn,
                    kbid,
                    semantic_model=model,
                    release_channel=release_channel,
                )
            await kb.set_resource_shard_id(uuid, shard.shard)

        if shard is not None:
            index_message = resource.indexer.brain
            await self.shard_manager.add_resource(
                shard,
                index_message,
                seqid,
                partition=partition,
                kb=kbid,
                source=source,
            )
        else:
            raise AttributeError("Shard is not available")

    async def multi(self, message: writer_pb2.BrokerMessage, seqid: int) -> None:
        self.messages.setdefault(message.multiid, []).append(message)

    async def commit(
        self, message: writer_pb2.BrokerMessage, seqid: int, partition: str
    ) -> None:
        if message.multiid not in self.messages:
            # Error
            logger.error(f"Closed multi {message.multiid}")
            await self.deadletter([message], partition, seqid)
        else:
            await self.txn(self.messages[message.multiid], seqid, partition)

    async def rollback(
        self, message: writer_pb2.BrokerMessage, seqid: int, partition: str
    ) -> None:
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
        kb: KnowledgeBox,
        resource: Optional[Resource] = None,
    ) -> Optional[tuple[Resource, bool]]:
        """
        Convert a broker message into a resource object, and apply it to the database
        """
        created = False

        if resource is None:
            # Make sure we load the resource in case it already exists on db
            if message.uuid is None and message.slug:
                uuid = await kb.get_resource_uuid_by_slug(message.slug)
            else:
                uuid = message.uuid
            resource = await kb.get(uuid)

        if resource is None and message.source is message.MessageSource.WRITER:
            # It's a new resource
            resource = await kb.add_resource(uuid, message.slug, message.basic)
            created = True
        elif resource is not None:
            # It's an update of an existing resource, can come either from writer or
            # from processing
            await self.maybe_update_resource_basic(resource, message)
        elif resource is None and message.source is message.MessageSource.PROCESSOR:
            # It's a new resource, and somehow we received the message coming from processing before
            # the "fast" one, this shouldn't happen
            logger.info(
                f"Secondary message for resource {message.uuid} and resource does not exist, ignoring"
            )
            return None

        if resource is None:
            return None

        if message.HasField("origin"):
            await resource.set_origin(message.origin)

        if message.HasField("extra"):
            await resource.set_extra(message.extra)

        if message.HasField("security"):
            await resource.set_security(message.security)

        await resource.apply_fields(message)
        await resource.apply_extracted(message)
        return (resource, created)

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

    async def notify_commit(
        self,
        *,
        partition: str,
        seqid: int,
        multi: str,
        message: writer_pb2.BrokerMessage,
        write_type: writer_pb2.Notification.WriteType.ValueType,
    ):
        notification = writer_pb2.Notification(
            partition=int(partition),
            seqid=seqid,
            multi=multi,
            uuid=message.uuid,
            kbid=message.kbid,
            action=writer_pb2.Notification.Action.COMMIT,
            write_type=write_type,
            source=MESSAGE_TO_NOTIFICATION_SOURCE[message.source],
            # including the message here again might feel a bit unusual but allows
            # us to react to these notifications with the original payload
            message=message,
            processing_errors=len(message.errors) > 0,
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
            logger.info(
                f"Skip when resource does not even have basic metadata: {resource}"
            )
            return
        try:
            async with self.driver.transaction() as txn:
                kb.txn = resource.txn = txn

                shard_id = await kb.get_resource_shard_id(resource.uuid)
                shard = None
                if shard_id is not None:
                    shard = await kb.get_resource_shard(shard_id)
                if shard is None:
                    logger.warning(
                        "Unable to mark resource as error, shard is None. "
                        "This should not happen so you did something special to get here."
                    )
                    return

                resource.basic.metadata.status = resources_pb2.Metadata.Status.ERROR
                await resource.set_basic(resource.basic)
                await txn.commit()

            resource.indexer.set_processing_status(
                basic=resource.basic, previous_status=resource._previous_status
            )
            await self.shard_manager.add_resource(
                shard, resource.indexer.brain, seqid, partition=partition, kb=kb.kbid
            )
        except Exception:
            logger.warning("Error while marking resource as error", exc_info=True)

    # KB tools
    # XXX: Why are these utility functions here?
    async def get_kb_obj(
        self, txn: Transaction, kbid: knowledgebox_pb2.KnowledgeBoxID
    ) -> Optional[KnowledgeBox]:
        uuid: Optional[str] = kbid.uuid
        if uuid == "":
            uuid = await KnowledgeBox.get_kb_uuid(txn, kbid.slug)

        if uuid is None:
            return None

        if not (await KnowledgeBox.exist_kb(txn, uuid)):
            return None

        storage = await get_storage()
        kbobj = KnowledgeBox(txn, storage, uuid)
        return kbobj

    @processor_observer.wrap({"type": "create_kb"})
    async def create_kb(
        self,
        slug: str,
        config: Optional[knowledgebox_pb2.KnowledgeBoxConfig],
        semantic_model: knowledgebox_pb2.SemanticModelMetadata,
        forceuuid: Optional[str] = None,
        release_channel: utils_pb2.ReleaseChannel.ValueType = utils_pb2.ReleaseChannel.STABLE,
    ) -> str:
        async with self.driver.transaction() as txn:
            try:
                uuid, failed = await KnowledgeBox.create(
                    txn,
                    slug,
                    semantic_model,
                    uuid=forceuuid,
                    config=config,
                    release_channel=release_channel,
                )
                if failed:
                    raise Exception("Failed to create KB")
                await txn.commit()
                return uuid
            except KnowledgeBoxConflict:
                raise
            except Exception as e:
                errors.capture_exception(e)
                raise e

    async def update_kb(
        self,
        kbid: str,
        slug: str,
        config: Optional[knowledgebox_pb2.KnowledgeBoxConfig],
    ) -> str:
        txn = await self.driver.begin()
        try:
            uuid = await KnowledgeBox.update(txn, kbid, slug, config=config)
        except Exception as e:
            await txn.abort()
            raise e
        await txn.commit()
        return uuid

    async def delete_kb(self, kbid: str = "", slug: str = "") -> str:
        txn = await self.driver.begin()
        try:
            uuid = await KnowledgeBox.delete_kb(txn, kbid=kbid, slug=slug)
        except (AttributeError, KeyError, KnowledgeBoxNotFound) as exc:
            await txn.abort()
            raise exc
        await txn.commit()
        return uuid


def messages_source(messages: list[writer_pb2.BrokerMessage]):
    from_writer = all(
        (
            message.source == writer_pb2.BrokerMessage.MessageSource.WRITER
            for message in messages
        )
    )
    from_processor = all(
        (
            message.source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR
            for message in messages
        )
    )
    if from_writer:
        source = nodewriter_pb2.IndexMessageSource.WRITER
    elif from_processor:
        source = nodewriter_pb2.IndexMessageSource.PROCESSOR
    else:  # pragma: nocover
        msg = "Processor received multiple broker messages with different sources in the same txn!"
        logger.error(msg)
        errors.capture_exception(Exception(msg))
        source = nodewriter_pb2.IndexMessageSource.PROCESSOR
    return source
