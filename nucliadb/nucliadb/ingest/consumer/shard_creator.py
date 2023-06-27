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

import logging
import uuid
from functools import partial

from nucliadb.common.cluster.manager import choose_node
from nucliadb.common.cluster.settings import settings
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos import noderesources_pb2, nodesidecar_pb2, writer_pb2
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.storages.storage import Storage

from . import metrics
from .utils import DelayedTaskHandler

logger = logging.getLogger(__name__)


class ShardCreatorHandler:
    """
    The purpose of this component is to automatically create new shards
    when all shards in a kb are over configured desired size.
    """

    subscription_id: str

    def __init__(
        self,
        *,
        driver: Driver,
        storage: Storage,
        pubsub: PubSubDriver,
        check_delay: float = 10.0,
    ):
        self.driver = driver
        self.storage = storage
        self.pubsub = pubsub
        self.shard_manager = get_shard_manager()
        self.task_handler = DelayedTaskHandler(check_delay)

    async def initialize(self) -> None:
        self.subscription_id = str(uuid.uuid4())
        await self.task_handler.initialize()
        await self.pubsub.subscribe(
            handler=self.handle_message,
            key=const.PubSubChannels.RESOURCE_NOTIFY.format(kbid="*"),
            group="shard-creator",
            subscription_id=self.subscription_id,
        )

    async def finalize(self) -> None:
        await self.pubsub.unsubscribe(self.subscription_id)
        await self.task_handler.finalize()

    async def handle_message(self, raw_data) -> None:
        data = self.pubsub.parse(raw_data)
        notification = writer_pb2.Notification()
        notification.ParseFromString(data)

        if notification.action != writer_pb2.Notification.Action.INDEXED:
            metrics.total_messages.inc({"type": "shard_creator", "action": "ignored"})
            return

        self.task_handler.schedule(
            notification.kbid, partial(self.process_kb, notification.kbid)
        )
        metrics.total_messages.inc({"type": "shard_creator", "action": "scheduled"})

    def should_create_new_shard(self, counter: nodesidecar_pb2.Counter) -> bool:
        return counter.paragraphs > settings.max_shard_paragraphs

    @metrics.handler_histo.wrap({"type": "shard_creator"})
    async def process_kb(self, kbid: str) -> None:
        logger.info({"message": "Processing notification for kbid", "kbid": kbid})
        kb_shards = await self.shard_manager.get_shards_by_kbid_inner(kbid)
        current_shard: writer_pb2.ShardObject = kb_shards.shards[kb_shards.actual]

        node, shard_id, _ = choose_node(current_shard)
        shard_counter: nodesidecar_pb2.Counter = await node.sidecar.GetCount(
            noderesources_pb2.ShardId(id=shard_id)  # type: ignore
        )
        if self.should_create_new_shard(shard_counter):
            logger.warning({"message": "Adding shard", "kbid": kbid})
            async with self.driver.transaction() as txn:
                kb = KnowledgeBox(txn, self.storage, kbid)
                model = await kb.get_model_metadata()
                await self.shard_manager.create_shard_by_kbid(
                    txn, kbid, semantic_model=model
                )
                await txn.commit()
