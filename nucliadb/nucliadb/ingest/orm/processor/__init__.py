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
from typing import Dict, List, Optional, Tuple

import aiohttp.client_exceptions
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBox as KnowledgeBoxPB
from nucliadb_protos.knowledgebox_pb2 import (
    KnowledgeBoxConfig,
    KnowledgeBoxID,
    KnowledgeBoxResponseStatus,
)
from nucliadb_protos.resources_pb2 import Metadata as PBMetadata
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_protos.writer_pb2 import BrokerMessage, Notification

from nucliadb.ingest.maindb.driver import Driver, Transaction
from nucliadb.ingest.orm.exceptions import (
    DeadletteredError,
    KnowledgeBoxConflict,
    KnowledgeBoxNotFound,
    SequenceOrderViolation,
)
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb.ingest.orm.processor import sequence_manager
from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.orm.shard import Shard
from nucliadb.ingest.orm.utils import get_node_klass
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.cache.utility import Cache
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage

logger = logging.getLogger(__name__)


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

    messages: Dict[str, List[BrokerMessage]]

    def __init__(
        self,
        driver: Driver,
        storage: Storage,
        cache: Optional[Cache] = None,
        partition: Optional[str] = None,
    ):
        self.messages = {}
        self.driver = driver
        self.storage = storage
        self.partition = partition
        self.cache = cache

    async def process(
        self,
        message: BrokerMessage,
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

        if message.type == BrokerMessage.MessageType.DELETE:
            await self.delete_resource(message, seqid, partition, transaction_check)
        elif message.type == BrokerMessage.MessageType.AUTOCOMMIT:
            await self.txn([message], seqid, partition, transaction_check)
        elif message.type == BrokerMessage.MessageType.MULTI:
            # XXX Not supported right now
            # MULTI, COMMIT and ROLLBACK are all not supported in transactional mode right now
            # This concept is probably not tenable with current architecture because
            # of how nats works and how we would need to manage rollbacks.
            # XXX Should this be removed?
            await self.multi(message, seqid)
        elif message.type == BrokerMessage.MessageType.COMMIT:
            await self.commit(message, seqid, partition)
        elif message.type == BrokerMessage.MessageType.ROLLBACK:
            await self.rollback(message, seqid, partition)

    async def get_resource_uuid(self, kb: KnowledgeBox, message: BrokerMessage) -> str:
        if message.uuid is None:
            uuid = await kb.get_resource_uuid_by_slug(message.slug)
        else:
            uuid = message.uuid
        return uuid

    @processor_observer.wrap({"type": "delete_resource"})
    async def delete_resource(
        self,
        message: BrokerMessage,
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
            node_klass = get_node_klass()
            shard: Optional[Shard] = await kb.get_resource_shard(shard_id, node_klass)
            if shard is None:
                raise AttributeError("Shard not available")
            await shard.delete_resource(message.uuid, seqid, partition, message.kbid)
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
            write_type=Notification.WriteType.DELETED,
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
        messages: List[BrokerMessage],
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
        shard: Optional[Shard] = None

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
                if message.reindex:
                    # when reindexing, let's just generate full new index message
                    resource.replace_indexer(await resource.generate_index_message())

            if resource and resource.modified:
                shard = await self.index_resource(  # noqa
                    resource=resource,
                    txn=txn,
                    uuid=uuid,
                    kbid=kbid,
                    seqid=seqid,
                    partition=partition,
                    kb=kb,
                )

                if transaction_check:
                    await sequence_manager.set_last_seqid(txn, partition, seqid)
                await txn.commit()

                if created or resource.slug_modified:
                    await self.commit_slug(resource)

                await self.notify_commit(
                    partition=partition,
                    seqid=seqid,
                    multi=multi,
                    message=message,
                    write_type=Notification.WriteType.CREATED
                    if created
                    else Notification.WriteType.MODIFIED,
                )
            elif resource and resource.modified is False:
                await txn.abort()
                await self.notify_abort(
                    partition=partition, seqid=seqid, multi=multi, kbid=kbid, rid=uuid
                )
                logger.warning(f"This message did not modify the resource")
        except (
            asyncio.TimeoutError,
            asyncio.CancelledError,
            aiohttp.client_exceptions.ClientError,
        ):  # pragma: no cover
            # Unhandled exceptions here that should bubble and hard fail
            # XXX We swallow too many exceptions here!
            await self.notify_abort(
                partition=partition, seqid=seqid, multi=multi, kbid=kbid, rid=uuid
            )
            raise
        except Exception as exc:
            # As we are in the middle of a transaction, we cannot let the exception raise directly
            # as we need to do some cleanup. The exception will be reraised at the end of the function
            # and then handled by the top caller, so errors can be handled in the same place.
            await self.deadletter(messages, partition, seqid)
            await self.notify_abort(
                partition=partition, seqid=seqid, multi=multi, kbid=kbid, rid=uuid
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
                # Removing this until we figure out how to properly index the error state
                # await self._mark_resource_error(resource, partition, seqid, shard, kbid)
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
    ) -> Shard:
        shard_id = await kb.get_resource_shard_id(uuid)
        node_klass = get_node_klass()

        shard = None
        if shard_id is not None:
            shard = await kb.get_resource_shard(shard_id, node_klass)

        if shard is None:
            # It's a new resource, get current active shard to place
            # new resource on
            shard = await node_klass.get_current_active_shard(txn, kbid)
            if shard is None:
                # no shard available, create a new one
                similarity = await kb.get_similarity()
                shard = await node_klass.create_shard_by_kbid(
                    txn, kbid, similarity=similarity
                )
            await kb.set_resource_shard_id(uuid, shard.sharduuid)

        if shard is not None:
            await shard.add_resource(
                resource.indexer.brain, seqid, partition=partition, kb=kbid
            )
        else:
            raise AttributeError("Shard is not available")
        return shard

    async def _mark_resource_error(
        self,
        resource: Optional[Resource],
        partition: str,
        seqid: int,
        shard: Optional[Shard],
        kbid: str,
    ) -> None:
        """
        Unhandled error processing, try to mark resource as error
        """
        if shard is None:
            logger.warning(
                "Unable to mark resource as error, shard is None. "
                "This should not happen so you did something special to get here."
            )
            return
        if resource is None or resource.basic is None:
            logger.info(
                f"Skip when resource does not even have basic metadata: {resource}"
            )
            return
        prev_txn = resource.txn
        try:
            async with self.driver.transaction() as txn:
                resource.txn = txn
                resource.basic.metadata.status = PBMetadata.Status.ERROR
                await resource.set_basic(resource.basic)
                await txn.commit()

            await shard.add_resource(
                resource.indexer.brain, seqid, partition=partition, kb=kbid
            )
        except Exception:
            logger.warning("Error while marking resource as error", exc_info=True)
        finally:
            resource.txn = prev_txn

    async def multi(self, message: BrokerMessage, seqid: int) -> None:
        self.messages.setdefault(message.multiid, []).append(message)

    async def commit(self, message: BrokerMessage, seqid: int, partition: str) -> None:
        if message.multiid not in self.messages:
            # Error
            logger.error(f"Closed multi {message.multiid}")
            await self.deadletter([message], partition, seqid)
        else:
            await self.txn(self.messages[message.multiid], seqid, partition)

    async def rollback(
        self, message: BrokerMessage, seqid: int, partition: str
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
        )

    async def deadletter(
        self, messages: List[BrokerMessage], partition: str, seqid: int
    ) -> None:
        for seq, message in enumerate(messages):
            await self.storage.deadletter(message, seq, seqid, partition)

    @processor_observer.wrap({"type": "apply_resource"})
    async def apply_resource(
        self,
        message: BrokerMessage,
        kb: KnowledgeBox,
        resource: Optional[Resource] = None,
    ) -> Optional[Tuple[Resource, bool]]:
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
            if (
                message.HasField("basic")
                or message.slug != ""
                or len(message.delete_fields) > 0
            ):
                await resource.set_basic(
                    message.basic,
                    slug=message.slug,
                    deleted_fields=message.delete_fields,  # type: ignore
                )
        elif resource is None and message.source is message.MessageSource.PROCESSOR:
            # It's a new resource, and somehow we received the message coming from processing before
            # the "fast" one, this shouldn't happen
            logger.info(
                f"Secondary message for resource {message.uuid} and resource does not exist, ignoring"
            )
            return None

        if message.HasField("origin") and resource:
            await resource.set_origin(message.origin)

        if message.HasField("extra") and resource:
            await resource.set_extra(message.extra)

        if resource:
            await resource.apply_fields(message)
            await resource.apply_extracted(message)
            return (resource, created)

        return None

    async def notify_commit(
        self,
        *,
        partition: str,
        seqid: int,
        multi: str,
        message: BrokerMessage,
        write_type: Notification.WriteType.Value,  # type: ignore
    ):
        notification = Notification(
            partition=int(partition),
            seqid=seqid,
            multi=multi,
            uuid=message.uuid,
            kbid=message.kbid,
            action=Notification.Action.COMMIT,
            # including the message here again might feel a bit unusual but allows
            # us to react to these notifications with the original payload
            write_type=write_type,
            message=message,
        )

        await self.notify(
            const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=message.kbid),
            notification.SerializeToString(),
        )

    async def notify_abort(
        self, *, partition: str, seqid: int, multi: str, kbid: str, rid: str
    ):
        message = Notification(
            partition=int(partition),
            seqid=seqid,
            multi=multi,
            uuid=rid,
            kbid=kbid,
            action=Notification.ABORT,
        )
        await self.notify(
            const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=kbid),
            message.SerializeToString(),
        )

    async def notify(self, channel, payload: bytes):
        if self.cache is not None and self.cache.pubsub is not None:
            await self.cache.pubsub.publish(channel, payload)

    # KB tools
    # XXX: Why are these utility functions here?
    async def get_kb_obj(
        self, txn: Transaction, kbid: KnowledgeBoxID
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

    async def get_kb(self, slug: str = "", uuid: Optional[str] = "") -> KnowledgeBoxPB:
        txn = await self.driver.begin()

        if uuid == "" and slug != "":
            uuid = await KnowledgeBox.get_kb_uuid(txn, slug)

        response = KnowledgeBoxPB()
        if uuid is None:
            response.status = KnowledgeBoxResponseStatus.NOTFOUND
            await txn.abort()
            return response

        config = await KnowledgeBox.get_kb(txn, uuid)

        await txn.abort()

        if config is None:
            response.status = KnowledgeBoxResponseStatus.NOTFOUND
            return response

        response.uuid = uuid
        response.slug = config.slug
        response.config.CopyFrom(config)
        return response

    async def get_kb_uuid(self, slug: str) -> Optional[str]:
        txn = await self.driver.begin()
        uuid = await KnowledgeBox.get_kb_uuid(txn, slug)
        await txn.abort()
        return uuid

    async def create_kb(
        self,
        slug: str,
        config: Optional[KnowledgeBoxConfig],
        forceuuid: Optional[str] = None,
        similarity: VectorSimilarity.ValueType = VectorSimilarity.COSINE,
    ) -> str:
        async with self.driver.transaction() as txn:
            try:
                uuid, failed = await KnowledgeBox.create(
                    txn, slug, config=config, uuid=forceuuid, similarity=similarity
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
        self, kbid: str, slug: str, config: Optional[KnowledgeBoxConfig]
    ) -> str:
        txn = await self.driver.begin()
        try:
            uuid = await KnowledgeBox.update(txn, kbid, slug, config=config)
        except Exception as e:
            await txn.abort()
            raise e
        await txn.commit()
        return uuid

    async def list_kb(self, prefix: str):
        txn = await self.driver.begin()
        async for kbid, slug in KnowledgeBox.get_kbs(txn, prefix):
            yield slug
        await txn.abort()

    async def delete_kb(self, kbid: str = "", slug: str = "") -> str:
        txn = await self.driver.begin()
        try:
            uuid = await KnowledgeBox.delete_kb(txn, kbid=kbid, slug=slug)
        except (AttributeError, KeyError, KnowledgeBoxNotFound) as exc:
            await txn.abort()
            raise exc
        await txn.commit()
        return uuid
