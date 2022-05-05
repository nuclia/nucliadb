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
from typing import Dict, List, Optional

from nucliadb_protos.knowledgebox_pb2 import KnowledgeBox as KnowledgeBoxPB
from nucliadb_protos.knowledgebox_pb2 import (
    KnowledgeBoxConfig,
    KnowledgeBoxID,
    KnowledgeBoxResponseStatus,
    Widget,
)
from nucliadb_protos.writer_pb2 import BrokerMessage, Notification

from nucliadb_ingest import logger
from nucliadb_ingest.maindb.driver import Driver, Transaction
from nucliadb_ingest.orm.exceptions import DeadletteredError
from nucliadb_ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_ingest.orm.resource import Resource
from nucliadb_ingest.orm.shard import Shard
from nucliadb_ingest.orm.utils import get_node_klass
from nucliadb_ingest.settings import settings
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.cache.utility import Cache
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_cache, get_storage

DEFAULT_WIDGET = Widget(id="dashboard", mode=Widget.WidgetMode.INPUT)
DEFAULT_WIDGET.features.useFilters = True
DEFAULT_WIDGET.features.suggestEntities = True
DEFAULT_WIDGET.features.suggestSentences = True
DEFAULT_WIDGET.features.suggestParagraphs = True


class Processor:
    messages: Dict[str, List[BrokerMessage]]

    def __init__(
        self,
        driver: Driver,
        storage: Storage,
        audit: AuditStorage,
        cache: Cache,
        partition: Optional[str] = None,
    ):
        self.messages = {}
        self.driver = driver
        self.storage = storage
        self.audit = audit
        self.partition = partition
        self.cache = cache

    async def initialize(self):
        await self.driver.initialize()
        if self.cache is not None:
            await self.cache.initialize()

    async def finalize(self):
        await self.driver.finalize()
        if self.cache is not None:
            await self.cache.finalize()

    async def process(
        self,
        message: BrokerMessage,
        seqid: int,
        partition: Optional[str] = None,
    ) -> bool:
        partition = partition if self.partition is None else self.partition
        if partition is None:
            raise AttributeError()

        # check seqid > last txid on partition
        last_seq = await self.driver.last_seqid(partition)
        if last_seq is not None and seqid <= last_seq:
            return False

        if message.type == BrokerMessage.MessageType.DELETE:
            await self.delete_resource(message, seqid, partition)
        elif message.type == BrokerMessage.MessageType.AUTOCOMMIT:
            await self.autocommit(message, seqid, partition)
        elif message.type == BrokerMessage.MessageType.MULTI:
            await self.multi(message, seqid)
        elif message.type == BrokerMessage.MessageType.COMMIT:
            await self.commit(message, seqid, partition)
        elif message.type == BrokerMessage.MessageType.ROLLBACK:
            await self.rollback(message, seqid, partition)
        await self.audit.report(message)
        return True

    async def get_resource_uuid(self, kb: KnowledgeBox, message: BrokerMessage) -> str:
        if message.uuid is None:
            uuid = await kb.get_resource_uuid_by_slug(message.slug)
        else:
            uuid = message.uuid
        return uuid

    async def delete_resource(self, message: BrokerMessage, seqid: int, partition: str):
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, message.kbid)

        uuid = await self.get_resource_uuid(kb, message)
        shard_id = await kb.get_resource_shard_id(uuid)
        if shard_id is None:
            logger.warn(f"Resource {uuid} does not exist")
        else:
            shard: Optional[Shard] = await kb.get_resource_shard(shard_id)
            if shard is None:
                raise AttributeError("Shard not available")
            await shard.delete_resource(message.uuid, seqid)
            try:
                await kb.delete_resource(message.uuid)
            except Exception as exc:
                await txn.abort()
                await self.notify_abort(
                    partition, seqid, message.multiid, message.kbid, message.uuid
                )
                raise exc
        if txn.open:
            await txn.commit(partition, seqid)
        await self.notify_commit(
            partition, seqid, message.multiid, message.kbid, message.uuid
        )

    def generate_index(self, resource: Resource, messages: List[BrokerMessage]):
        pass

    async def txn(self, messages: List[BrokerMessage], seqid: int, partition: str):
        if len(messages) == 0:
            return

        txn = await self.driver.begin()
        kbid = messages[0].kbid
        if not await KnowledgeBox.exist_kb(txn, kbid):
            logger.warn(f"KB {kbid} is deleted: skiping txn")
            await txn.commit(partition, seqid)
            return

        multi = messages[0].multiid
        kb = KnowledgeBox(txn, self.storage, self.cache, kbid)
        uuid = await self.get_resource_uuid(kb, messages[0])
        resource: Optional[Resource] = None
        handled_exception = None
        origin_txn = seqid

        try:
            for message in messages:
                if resource is not None:
                    assert resource.uuid == message.uuid

                resource = await self.apply_resource(message, kb, resource)

            if resource:
                await resource.compute_global_text()
                await resource.compute_global_tags(resource.indexer)

            if resource and resource.modified:
                shard_id = await kb.get_resource_shard_id(uuid)
                shard: Optional[Shard] = None
                if shard_id is not None:
                    shard = await kb.get_resource_shard(shard_id)

                node_klass = get_node_klass()
                if shard is None:
                    # Its a new resource
                    # Check if we have enough resource to create a new shard
                    shard = await node_klass.actual_shard(txn, kbid)
                    if shard is None:
                        shard = await node_klass.create_shard_by_kbid(txn, kbid)
                    await kb.set_resource_shard_id(uuid, shard.sharduuid)

                if shard is not None:
                    count = await shard.add_resource(resource.indexer.brain, seqid)
                    if count > settings.max_node_fields:
                        shard = await node_klass.create_shard_by_kbid(txn, kbid)

                else:
                    raise AttributeError("Shard is not available")

                await txn.commit(partition, seqid)

                # Slug may have conflicts as its not partitioned properly. We make it as short as possible
                txn = await self.driver.begin()
                resource.txn = txn
                await resource.set_slug()
                await txn.commit(resource=False)

                await self.notify_commit(partition, origin_txn, multi, kbid, uuid)

            elif resource and resource.modified is False:
                await txn.abort()
                await self.notify_abort(partition, origin_txn, multi, kbid, uuid)
        except Exception as exc:
            # As we are in the middle of a transaction, we cannot let the exception raise directly
            # as we need to do some cleanup. Exception will be reraised at the end of the function
            # and then handled by the top caller, so errors can be handled in the same place.
            await self.deadletter(messages, partition, seqid)
            await self.notify_abort(partition, origin_txn, multi, kbid, uuid)
            handled_exception = exc
        finally:
            if resource is not None:
                resource.clean()
            # tx should be already commited or aborted, but in the event of an exception
            # it could be left open. Make sure to close it it's still open
            if txn.open:
                await txn.abort()

        if handled_exception is not None:
            if seqid == -1:
                raise handled_exception
            else:
                raise DeadletteredError() from handled_exception

    async def autocommit(self, message: BrokerMessage, seqid: int, partition: str):
        await self.txn([message], seqid, partition)

    async def multi(self, message: BrokerMessage, seqid: int):
        self.messages.setdefault(message.multiid, []).append(message)

    async def commit(self, message: BrokerMessage, seqid: int, partition: str):
        if message.multiid not in self.messages:
            # Error
            logger.error(f"Closed multi {message.multiid}")
            await self.deadletter(self.messages[message.multiid], partition, seqid)
            del self.messages[message.multiid]
            return
        else:
            await self.txn([message], seqid, partition)

    async def rollback(self, message: BrokerMessage, seqid: int, partition: str):
        # Error
        logger.error(f"Closed multi {message.multiid}")
        del self.messages[message.multiid]
        await self.notify_abort(
            partition, seqid, message.multiid, message.kbid, message.uuid
        )

    async def deadletter(
        self, messages: List[BrokerMessage], partition: str, seqid: int
    ):
        for seq, message in enumerate(messages):
            await self.storage.deadletter(message, seq, seqid, partition)

    async def apply_resource(
        self,
        message: BrokerMessage,
        kb: KnowledgeBox,
        resource: Optional[Resource] = None,
    ):
        if resource is None:
            if message.uuid is None and message.slug:
                uuid = await kb.get_resource_uuid_by_slug()
            else:
                uuid = message.uuid
            resource = await kb.get(uuid)
        if resource is None and message.txseqid == 0:
            # Its new and its not a secondary message
            resource = await kb.add_resource(uuid, message.slug, message.basic)
        elif resource is not None:
            # Already exists
            if message.HasField("basic") or message.slug != "":
                await resource.set_basic(message.basic, slug=message.slug)
        elif resource is None and message.txseqid > 0:
            # Does not exist and secondary message
            logger.info(
                f"Secondary message for resource {message.uuid} and resource does not exist, ignoring"
            )
            return None

        if message.origin and resource:
            await resource.set_origin(message.origin)

        if resource:
            await resource.apply_fields(message)
            await resource.apply_extracted(message)
        return resource

    async def notify_commit(
        self, partition: str, seqid: int, multi: str, kbid: str, uuid: str
    ):
        message = Notification(
            partition=int(partition),
            seqid=seqid,
            multi=multi,
            uuid=uuid,
            kbid=kbid,
            action=Notification.COMMIT,
        )
        await self.notify(f"notify.{kbid}", message.SerializeToString())

    async def notify_abort(
        self, partition: str, seqid: int, multi: str, kbid: str, uuid: str
    ):
        message = Notification(
            partition=int(partition),
            seqid=seqid,
            multi=multi,
            uuid=uuid,
            kbid=kbid,
            action=Notification.ABORT,
        )
        await self.notify(f"notify.{kbid}", message.SerializeToString())

    # KB tools

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
        cache = await get_cache()
        kbobj = KnowledgeBox(txn, storage, cache, uuid)
        return kbobj

    async def get_kb(self, slug: str = "", uuid: Optional[str] = "") -> KnowledgeBoxPB:
        txn = await self.driver.begin()

        if uuid == "" and slug != "":
            uuid = await KnowledgeBox.get_kb_uuid(txn, slug)

        response = KnowledgeBoxPB()
        if uuid is None:
            response.status = KnowledgeBoxResponseStatus.NOTFOUND
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
    ) -> str:
        txn = await self.driver.begin()
        try:
            uuid, failed = await KnowledgeBox.create(
                txn, slug, config=config, uuid=forceuuid
            )
        except Exception as e:
            await txn.abort()
            raise e

        if not failed:
            storage = await get_storage()
            cache = await get_cache()
            kb = KnowledgeBox(txn, storage, cache, uuid)
            await kb.set_widgets(DEFAULT_WIDGET)

        if failed:
            await txn.abort()
        else:
            await txn.commit(resource=False)
        return uuid

    async def update_kb(
        self, kbid: str, slug: str, config: Optional[KnowledgeBoxConfig]
    ) -> str:
        txn = await self.driver.begin()
        try:
            uuid = await KnowledgeBox.update(txn, kbid, slug, config=config)
        except Exception as e:
            await txn.abort()
            raise e
        await txn.commit(resource=False)
        return uuid

    async def list_kb(self, prefix: str):
        txn = await self.driver.begin()
        async for slug in KnowledgeBox.get_kbs(txn, prefix):
            yield slug
        await txn.abort()

    async def delete_kb(self, kbid: str = "", slug: str = "") -> str:
        txn = await self.driver.begin()
        uuid = await KnowledgeBox.delete_kb(txn, kbid=kbid, slug=slug)
        await txn.commit(resource=False)
        return uuid

    async def notify(self, channel, payload: bytes):
        if self.cache is not None and self.cache.pubsub is not None:
            await self.cache.pubsub.publish(channel, payload)
