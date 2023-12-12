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

import nats
from nats.aio.client import Msg
from nats.aio.subscription import Subscription
from nats.js.errors import NotFoundError as StreamNotFoundError

from nucliadb_node import SERVICE_NAME, logger
from nucliadb_node.indexer import ConcurrentShardIndexer
from nucliadb_node.writer import Writer
from nucliadb_utils import const
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage


class Worker:
    subscriptions: list[Subscription]
    storage: Storage

    def __init__(
        self,
        writer: Writer,
        node: str,
        nats_connection_manager: NatsConnectionManager,
    ):
        self.writer = writer
        self.subscriptions = []
        self.node = node
        self.indexer = ConcurrentShardIndexer(writer, node)
        self.nats_connection_manager = nats_connection_manager

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)
        await self.indexer.initialize()
        await self.setup_nats_subscription()

    async def finalize(self):
        await self.teardown_nats_subscriptions()
        await self.indexer.finalize()

    async def setup_nats_subscription(self):
        try:
            await self.nats_connection_manager.js.stream_info(const.Streams.INDEX.name)
        except StreamNotFoundError:
            logger.info("Creating stream")
            await self.nats_connection_manager.js.add_stream(
                name=const.Streams.INDEX.name,
                subjects=[
                    const.Streams.INDEX.subject.format(node=">"),
                ],
            )
            await self.nats_connection_manager.js.stream_info(const.Streams.INDEX.name)

        subject = const.Streams.INDEX.subject.format(node=self.node)
        subscription = await self.nats_connection_manager.subscribe(
            subject=subject,
            queue=const.Streams.INDEX.group.format(node=self.node),
            stream=const.Streams.INDEX.name,
            flow_control=True,
            manual_ack=True,
            cb=self.subscription_worker,
            subscription_lost_cb=self.setup_nats_subscription,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.ALL,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_ack_pending=nats_consumer_settings.nats_max_ack_pending,
                max_deliver=nats_consumer_settings.nats_max_deliver,
                ack_wait=nats_consumer_settings.nats_ack_wait,
                idle_heartbeat=nats_consumer_settings.nats_idle_heartbeat,
            ),
        )
        self.subscriptions.append(subscription)
        logger.info(f"Subscribed to {subject} on stream {const.Streams.INDEX.name}")

    async def teardown_nats_subscriptions(self):
        for subscription in self.subscriptions:
            await self.nats_connection_manager.unsubscribe(subscription)
        self.subscriptions.clear()

    async def subscription_worker(self, msg: Msg):
        self.indexer.index_message_soon(msg)
