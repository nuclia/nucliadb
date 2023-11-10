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
from nucliadb_node.settings import settings
from nucliadb_node.writer import Writer
from nucliadb_telemetry import metrics
from nucliadb_utils import const
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage

subscriber_observer = metrics.Observer(
    "message_processor",
    buckets=[
        0.01,
        0.025,
        0.05,
        0.1,
        0.5,
        1.0,
        2.5,
        5.0,
        7.5,
        10.0,
        30.0,
        60.0,
        120.0,
        float("inf"),
    ],
)


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
        self.indexer = ConcurrentShardIndexer(self.writer)
        self.nats_connection_manager = nats_connection_manager

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)
        await self.indexer.initialize()
        await self.setup_nats_subscription()

    async def finalize(self):
        await self.indexer.finalize()

    async def setup_nats_subscription(self):
        self.load_seqid()
        logger.info(f"Last seqid {self.last_seqid}")

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
        await self.nats_connection_manager.subscribe(
            subject=subject,
            queue=const.Streams.INDEX.group.format(node=self.node),
            stream=const.Streams.INDEX.name,
            flow_control=True,
            manual_ack=True,
            cb=self.subscription_worker,
            subscription_lost_cb=self.setup_nats_subscription,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=self.last_seqid or 1,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_ack_pending=nats_consumer_settings.nats_max_ack_pending,
                max_deliver=nats_consumer_settings.nats_max_deliver,
                ack_wait=nats_consumer_settings.nats_ack_wait,
                idle_heartbeat=nats_consumer_settings.nats_idle_heartbeat,
            ),
        )
        logger.info(f"Subscribed to {subject} on stream {const.Streams.INDEX.name}")

    def store_seqid(self, seqid: int):
        if settings.data_path is None:
            raise Exception("We need a DATA_PATH env")
        with open(f"{settings.data_path}/seqid", "w+") as seqfile:
            seqfile.write(str(seqid))
        self.last_seqid = seqid

    def load_seqid(self):
        if settings.data_path is None:
            raise Exception("We need a DATA_PATH env")
        try:
            with open(f"{settings.data_path}/seqid", "r") as seqfile:
                self.last_seqid = int(seqfile.read())
        except FileNotFoundError:
            # First time the consumer is started
            self.last_seqid = None

    @subscriber_observer.wrap()
    async def subscription_worker(self, msg: Msg):
        seqid = int(msg.reply.split(".")[5])

        if self.last_seqid and self.last_seqid >= seqid:
            logger.warning(
                f"Skipping already processed message. Msg seqid {seqid} vs Last seqid {self.last_seqid}"
            )
            await msg.ack()
            return

        self.indexer.index_message_nowait(msg)

        # TODO? accounting for current indexing, manage seqid
