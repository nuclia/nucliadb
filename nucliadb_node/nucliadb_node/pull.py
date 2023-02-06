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
from nucliadb_protos.noderesources_pb2 import Resource, ResourceID, ShardIds
from nucliadb_protos.nodewriter_pb2 import IndexMessage
from sentry_sdk import capture_exception

from nucliadb_node import SERVICE_NAME, logger
from nucliadb_node.reader import Reader
from nucliadb_node.sentry import SENTRY
from nucliadb_node.settings import settings
from nucliadb_node.writer import Writer
from nucliadb_telemetry.jetstream import JetStreamContextTelemetry
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_storage,
    get_transaction,
)


class Worker:
    subscriptions: List[Subscription]

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

    async def finalize(self):
        if self.gc_task:
            self.gc_task.cancel()
        for subscription in self.subscriptions:
            try:
                await subscription.drain()
            except nats.errors.ConnectionClosedError:
                pass
        self.subscriptions = []
        transaction_utility = get_transaction()
        if transaction_utility:
            await transaction_utility.finalize()
            clean_utility(Utility.TRANSACTION)

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.info("Got reconnected to NATS {url}".format(url=self.nc.connected_url))

    async def error_cb(self, e):
        capture_exception(e)
        logger.error("There was an error on the worker, check sentry: {}".format(e))

    async def closed_cb(self):
        logger.info("Connection is closed on NATS")

    async def initialize(self):
        self.event.clear()
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

        tracer_provider = get_telemetry(SERVICE_NAME)
        jetstream = self.nc.jetstream()
        if tracer_provider is not None:
            logger.info("Configuring node queue with telemetry")
            self.js = JetStreamContextTelemetry(
                jetstream, f"{SERVICE_NAME}_js_worker", tracer_provider
            )
        else:
            self.js = jetstream

        logger.info(f"Nats: Connected to {indexing_settings.index_jetstream_servers}")
        await self.subscribe()
        self.gc_task = asyncio.create_task(self.garbage())

    async def garbage(self) -> None:
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

    def load_seqid(self) -> Optional[int]:
        if settings.data_path is None:
            raise Exception("We need a DATA_PATH env")
        try:
            with open(f"{settings.data_path}/seqid", "r") as seqfile:
                return int(seqfile.read())
        except FileNotFoundError:
            return None

    async def subscription_worker(self, msg: Msg):
        subject = msg.subject
        reply = msg.reply
        seqid = int(msg.reply.split(".")[5])
        logger.info(
            f"Message received: subject:{subject}, seqid: {seqid}, reply: {reply}"
        )
        storage = await get_storage(service_name=SERVICE_NAME)
        self.event.clear()
        async with self.lock:
            try:
                pb = IndexMessage()
                pb.ParseFromString(msg.data)
                if pb.typemessage == IndexMessage.TypeMessage.CREATION:
                    brain: Resource = await storage.get_indexing(pb)
                    logger.info(
                        f"Added {brain.resource.uuid} at {brain.shard_id} otx:{pb.txid}"
                    )
                    status = await self.writer.set_resource(brain)
                    logger.info(f"...done")

                    del brain
                elif pb.typemessage == IndexMessage.TypeMessage.DELETION:
                    rid = ResourceID()
                    rid.shard_id = pb.shard
                    rid.uuid = pb.resource
                    logger.info(f"Deleting {pb.resource} otx:{pb.txid}")
                    status = await self.writer.delete_resource(rid)
                    logger.info(f"...done")
                self.reader.update(pb.shard, status)

            except AioRpcError as grpc_error:
                if grpc_error.code() == StatusCode.NOT_FOUND:
                    logger.error(f"Shard does not exit {pb.shard}")
                else:
                    event_id: Optional[str] = None
                    if SENTRY:
                        event_id = capture_exception(grpc_error)
                    logger.error(
                        f"An error on subscription_worker. Check sentry for more details. Event id: {event_id}"
                    )
                    raise grpc_error

            except KeyError as storage_error:
                if SENTRY:
                    capture_exception(storage_error)
                logger.warn(
                    "Error retrieving the indexing payload we do not block as that means its already deleted"
                )
            except Exception as e:
                event_id = None
                if SENTRY:
                    event_id = capture_exception(e)
                logger.error(
                    f"An error on subscription_worker. Check sentry for more details. Event id: {event_id}"
                )
                raise e
        try:
            self.store_seqid(seqid)
            await msg.ack()
            self.event.set()
            await storage.delete_indexing(pb)
        except Exception as e:
            if SENTRY:
                capture_exception(e)
            logger.error(
                f"An error on subscription_worker. Check sentry for more details."
            )
            raise e

    async def subscribe(self):
        last_seqid = self.load_seqid()
        logger.info(f"Last seqid {last_seqid}")
        if last_seqid is None:
            last_seqid = 1

        try:
            await self.js.stream_info(indexing_settings.index_jetstream_stream)
        except nats.js.errors.NotFoundError:
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
                opt_start_seq=last_seqid,
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
