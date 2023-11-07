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

import asyncio

import nats
from nats.aio.client import Msg
from nats.aio.subscription import Subscription
from nats.js.errors import NotFoundError as StreamNotFoundError

from nucliadb_node import SERVICE_NAME, logger
from nucliadb_node.indexer import ConcurrentShardIndexer
from nucliadb_node.settings import indexing_settings, settings
from nucliadb_node.writer import Writer
from nucliadb_telemetry import errors, metrics
from nucliadb_utils import const
from nucliadb_utils.nats import get_traced_jetstream
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

    def __init__(self, writer: Writer, node: str):
        self.writer = writer
        self.subscriptions = []
        self.node = node
        self.load_seqid()
        self.indexer = ConcurrentShardIndexer(self.writer)

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)
        await self.indexer.initialize()
        await self.subscriber_initialize()

    async def finalize(self):
        await self.subscriber_finalize()
        await self.indexer.finalize()

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect
        logger.warning(
            f"Got reconnected to NATS {self.nc.connected_url}. Attempting reconnect"
        )
        await self.drain_subscriptions()
        await self.subscribe()

    async def error_cb(self, e):
        errors.capture_exception(e)
        logger.error(
            "There was an error on the worker, check sentry: {}".format(e),
            exc_info=True,
        )

    async def closed_cb(self):
        logger.info("Connection is closed on NATS")

    async def subscriber_initialize(self):
        options = {
            "error_cb": self.error_cb,
            "closed_cb": self.closed_cb,
            "reconnected_cb": self.reconnected_cb,
        }

        if indexing_settings.index_jetstream_auth:
            options["user_credentials"] = indexing_settings.index_jetstream_auth

        if len(indexing_settings.index_jetstream_servers) > 0:
            options["servers"] = indexing_settings.index_jetstream_servers

        self.nc = await nats.connect(**options)
        self.js = get_traced_jetstream(self.nc, SERVICE_NAME)
        logger.info(f"Nats: Connected to {indexing_settings.index_jetstream_servers}")
        await self.subscribe()

    async def drain_subscriptions(self) -> None:
        for subscription in self.subscriptions:
            try:
                await subscription.drain()
            except nats.errors.ConnectionClosedError:
                pass
        self.subscriptions = []

    async def subscriber_finalize(self):
        await self.drain_subscriptions()
        try:
            await self.nc.close()
        except (RuntimeError, AttributeError):  # pragma: no cover
            # RuntimeError: can be thrown if event loop is closed
            # AttributeError: can be thrown by nats-py when handling shutdown
            pass

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

    async def subscribe(self):
        logger.info(f"Last seqid {self.last_seqid}")
        try:
            await self.js.stream_info(const.Streams.INDEX.name)
        except StreamNotFoundError:
            logger.info("Creating stream")
            res = await self.js.add_stream(
                name=const.Streams.INDEX.name,
                subjects=[
                    const.Streams.INDEX.subject.format(node=">"),
                ],
            )
            await self.js.stream_info(const.Streams.INDEX.name)

        subject = const.Streams.INDEX.subject.format(node=self.node)
        res = await self.js.subscribe(
            subject=subject,
            queue=const.Streams.INDEX.group.format(node=self.node),
            stream=const.Streams.INDEX.name,
            flow_control=True,
            cb=self.subscription_worker,
            manual_ack=True,
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
        self.subscriptions.append(res)
        logger.info(f"Subscribed to {subject} on stream {const.Streams.INDEX.name}")

    @subscriber_observer.wrap()
    async def subscription_worker(self, msg: Msg):
        seqid = int(msg.reply.split(".")[5])
        if self.last_seqid and self.last_seqid >= seqid:
            logger.warning(
                f"Skipping already processed message. Msg seqid {seqid} vs Last seqid {self.last_seqid}"
            )
            await msg.ack()
            return

        def done_callback(task):
            if task.exception() is not None:
                error = task.exception()
                event_id = errors.capture_exception(error)
                logger.error(
                    f"An error on indexer task. Check sentry for more details. Event id: {event_id}",
                    exc_info=True,
                )
                raise error

        task = asyncio.create_task(self.indexer.index_message(msg))
        task.add_done_callback(done_callback)

        # TODO? accounting for current indexing, manage seqid
