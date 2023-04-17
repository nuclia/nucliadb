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

# We need to pull from jetstream key partition

import asyncio
from typing import List, Optional

import nats
from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore
from nats.aio.client import Msg
from nats.aio.subscription import Subscription
from nats.js.errors import NotFoundError as StreamNotFoundError
from nucliadb_protos.noderesources_pb2 import Resource, ResourceID, ShardIds
from nucliadb_protos.nodewriter_pb2 import (
    IndexedMessage,
    IndexMessage,
    OpStatus,
    TypeMessage,
)
from nucliadb_telemetry import errors, metrics
from nucliadb_utils.nats import get_traced_jetstream
from nucliadb_utils.storages.exceptions import IndexDataNotFound
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_storage,
    get_transaction_utility,
)

from nucliadb_node import SERVICE_NAME, logger, shadow_shards
from nucliadb_node.reader import Reader
from nucliadb_node.settings import indexing_settings, settings
from nucliadb_node.writer import Writer

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
    subscriptions: List[Subscription]
    storage: Storage

    def __init__(
        self,
        writer: Writer,
        reader: Reader,
        node: str,
    ):
        self.writer = writer
        self.reader = reader
        self.subscriptions = []
        self.ack_wait = 10 * 60
        self.lock = asyncio.Lock()
        self.event = asyncio.Event()
        self.node = node
        self.gc_task = None
        self.ssm = shadow_shards.get_manager()
        self.publisher = IndexedPublisher(
            stream=indexing_settings.indexed_jetstream_stream,
            subject=indexing_settings.indexed_jetstream_target,
            auth=indexing_settings.index_jetstream_auth,
            servers=indexing_settings.index_jetstream_servers,
        )
        self.load_seqid()

    async def finalize(self):
        if self.gc_task:
            self.gc_task.cancel()

        await self.subscriber_finalize()

        transaction_utility = get_transaction_utility()
        if transaction_utility:
            await transaction_utility.finalize()
            clean_utility(Utility.TRANSACTION)

        await self.publisher.finalize()
        await self.storage.finalize()

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect
        logger.info("Got reconnected to NATS {url}".format(url=self.nc.connected_url))

    async def error_cb(self, e):
        errors.capture_exception(e)
        logger.error(
            "There was an error on the worker, check sentry: {}".format(e),
            exc_info=True,
        )

    async def closed_cb(self):
        logger.info("Connection is closed on NATS")

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)
        await self.ssm.load()
        self.event.clear()
        await self.subscriber_initialize()
        await self.publisher.initialize()
        self.gc_task = asyncio.create_task(self.garbage())

    async def subscriber_initialize(self):
        options = {
            "error_cb": self.error_cb,
            "closed_cb": self.closed_cb,
            "reconnected_cb": self.reconnected_cb,
        }

        if indexing_settings.index_jetstream_auth is not None:
            options["user_credentials"] = indexing_settings.index_jetstream_auth

        if len(indexing_settings.index_jetstream_servers) > 0:
            options["servers"] = indexing_settings.index_jetstream_servers

        self.nc = await nats.connect(**options)
        self.js = get_traced_jetstream(self.nc, SERVICE_NAME)
        logger.info(f"Nats: Connected to {indexing_settings.index_jetstream_servers}")
        await self.subscribe()

    async def subscriber_finalize(self):
        for subscription in self.subscriptions:
            try:
                await subscription.drain()
            except nats.errors.ConnectionClosedError:
                pass
        self.subscriptions = []

        try:
            await self.nc.close()
        except (RuntimeError, AttributeError):  # pragma: no cover
            # RuntimeError: can be thrown if event loop is closed
            # AttributeError: can be thrown by nats-py when handling shutdown
            pass

    async def garbage(self) -> None:
        try:
            await self._garbage()
        except (asyncio.CancelledError, RuntimeError):  # pragma: no cover
            return

    async def _garbage(self) -> None:
        while True:
            await self.event.wait()
            await asyncio.sleep(10)
            if self.event.is_set():
                async with self.lock:
                    try:
                        logger.info(f"Mr Propper working")
                        shards: ShardIds = await self.writer.shards()
                        for shard in shards.ids:
                            await self.writer.garbage_collector(shard)
                        logger.info(f"Garbaged {len(shards.ids)}")
                    except Exception:
                        logger.exception(
                            f"Could not garbage {shard.id}", stack_info=True
                        )
                await asyncio.sleep(24 * 3660)

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

    async def set_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
        brain: Resource = await self.storage.get_indexing(pb)
        brain.shard_id = brain.resource.shard_id = pb.shard
        is_shadow_shard = self.ssm.exists(pb.shard)
        logger.info(
            f"Added [shadow={is_shadow_shard}] {brain.resource.uuid} at {brain.shard_id} otx:{pb.txid}"
        )
        status: Optional[OpStatus] = None
        if is_shadow_shard:
            await self.ssm.set_resource(brain, pb.shard, pb.txid)
        else:
            status = await self.writer.set_resource(brain)
        logger.info(f"...done")
        del brain
        return status

    async def delete_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
        is_shadow_shard = self.ssm.exists(pb.shard)
        logger.info(f"Deleting [shadow={is_shadow_shard}] {pb.resource} otx:{pb.txid}")
        status: Optional[OpStatus] = None
        if is_shadow_shard:
            await self.ssm.delete_resource(pb.resource, pb.shard, pb.txid)
        else:
            rid = ResourceID(uuid=pb.resource, shard_id=pb.shard)
            status = await self.writer.delete_resource(rid)
        logger.info(f"...done")
        return status

    @subscriber_observer.wrap()
    async def subscription_worker(self, msg: Msg):
        subject = msg.subject
        reply = msg.reply
        seqid = int(msg.reply.split(".")[5])
        logger.info(
            f"Message received: subject:{subject}, seqid: {seqid}, reply: {reply}"
        )
        if self.last_seqid and self.last_seqid >= seqid:
            logger.warning(
                f"Skipping already processed message. Msg seqid {seqid} vs Last seqid {self.last_seqid}"
            )
            await msg.ack()
            return

        self.event.clear()
        status: Optional[OpStatus] = None
        async with self.lock:
            try:
                pb = IndexMessage()
                pb.ParseFromString(msg.data)
                if pb.typemessage == TypeMessage.CREATION:
                    status = await self.set_resource(pb)
                elif pb.typemessage == TypeMessage.DELETION:
                    status = await self.delete_resource(pb)
                if status:
                    self.reader.update(pb.shard, status)

            except AioRpcError as grpc_error:
                if grpc_error.code() == StatusCode.NOT_FOUND:
                    logger.error(f"Shard does not exist {pb.shard}")
                else:
                    event_id = errors.capture_exception(grpc_error)
                    logger.error(
                        f"An error on subscription_worker. Check sentry for more details. Event id: {event_id}"
                    )
                    raise grpc_error

            except IndexDataNotFound as storage_error:
                # This should never happen now.
                # Remove this block in the future once we're confident it's not needed.
                errors.capture_exception(storage_error)
                logger.warning(
                    "Error retrieving the indexing payload we do not block as that means its already deleted"
                )
            except Exception as e:
                event_id = errors.capture_exception(e)
                logger.error(
                    f"An error on subscription_worker. Check sentry for more details. Event id: {event_id}"
                )
                raise e
        try:
            self.store_seqid(seqid)
            await msg.ack()
            self.event.set()
            await self.publisher.indexed(pb)
        except Exception as e:  # pragma: no cover
            errors.capture_exception(e)
            logger.error(
                f"An error on subscription_worker. Check sentry for more details."
            )
            raise e

    async def subscribe(self):
        logger.info(f"Last seqid {self.last_seqid}")
        try:
            await self.js.stream_info(indexing_settings.index_jetstream_stream)
        except StreamNotFoundError:
            logger.info("Creating stream")
            res = await self.js.add_stream(
                name=indexing_settings.index_jetstream_stream,
                subjects=[indexing_settings.index_jetstream_target.format(node=">")],
            )
            await self.js.stream_info(indexing_settings.index_jetstream_stream)

        res = await self.js.subscribe(
            subject=indexing_settings.index_jetstream_target.format(node=self.node),
            queue=indexing_settings.index_jetstream_group.format(node=self.node),
            stream=indexing_settings.index_jetstream_stream,
            flow_control=True,
            cb=self.subscription_worker,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=self.last_seqid or 1,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_deliver=10000,
                max_ack_pending=1,
                ack_wait=self.ack_wait,
                idle_heartbeat=5,
            ),
        )
        self.subscriptions.append(res)
        logger.info(
            f"Subscribed to {indexing_settings.index_jetstream_target.format(node=self.node)} on \
             stream {indexing_settings.index_jetstream_stream}"
        )


class IndexedPublisher:
    def __init__(
        self,
        stream: str,
        subject: str,
        servers: List[str],
        auth: Optional[str],
    ):
        self.stream = stream
        self.subject = subject
        self.auth: Optional[str] = auth
        self.servers: List[str] = servers
        self.js = None
        self.nc = None

    async def initialize(self):
        options = dict(
            closed_cb=self.on_connection_closed,
            reconnected_cb=self.on_reconnection,
        )
        if self.auth is not None:
            options["user_credentials"] = self.auth
        if len(self.servers) > 0:
            options["servers"] = self.servers
        self.nc = await nats.connect(**options)
        self.js = get_traced_jetstream(self.nc, SERVICE_NAME)
        await self.create_stream_if_not_exists()

    async def create_stream_if_not_exists(self):
        try:
            await self.js.stream_info(self.stream)
        except StreamNotFoundError:
            logger.info("Creating publisher stream")
            await self.js.add_stream(
                name=self.stream,
                subjects=[self.subject.format(partition=">")],
            )
            await self.js.stream_info(self.stream)

    async def on_reconnection(self):
        logger.warning(
            "Got reconnected to NATS {url}".format(url=self.nc.connected_url)
        )

    async def on_connection_closed(self):
        logger.warning("Connection is closed on NATS")

    async def finalize(self):
        if self.nc is not None:
            try:
                await self.nc.flush()
                await self.nc.close()
            except (RuntimeError, AttributeError):  # pragma: no cover
                # RuntimeError: can be thrown if event loop is closed
                # AttributeError: can be thrown by nats-py when handling shutdown
                pass
            self.nc = None

    async def indexed(self, indexpb: IndexMessage):
        if self.js is None:
            raise RuntimeError("Not initialized")

        if not indexpb.HasField("partition"):
            logger.warning(f"Could not publish message without partition")
            return

        indexedpb = IndexedMessage()
        indexedpb.node = indexpb.node
        indexedpb.shard = indexpb.shard
        indexedpb.txid = indexpb.txid
        indexedpb.resource = indexpb.resource
        indexedpb.typemessage = indexpb.typemessage
        indexedpb.reindex_id = indexpb.reindex_id
        subject = self.subject.format(partition=indexpb.partition)
        await self.js.publish(subject, indexedpb.SerializeToString())
        logger.info(f"Published to indexed stream {subject}")
        return
