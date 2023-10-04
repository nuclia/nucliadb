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

import time
from typing import List, Optional

import nats
from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore
from nats.aio.client import Msg
from nats.aio.subscription import Subscription
from nats.js.errors import NotFoundError as StreamNotFoundError
from nucliadb_protos.noderesources_pb2 import Resource, ResourceID
from nucliadb_protos.nodewriter_pb2 import IndexMessage, OpStatus, TypeMessage
from nucliadb_protos.writer_pb2 import Notification

from nucliadb_node import SERVICE_NAME, logger
from nucliadb_node.reader import Reader
from nucliadb_node.settings import indexing_settings, settings
from nucliadb_node.writer import Writer
from nucliadb_telemetry import errors, metrics
from nucliadb_utils import const
from nucliadb_utils.nats import MessageProgressUpdater, get_traced_jetstream
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.storages.exceptions import IndexDataNotFound
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_pubsub, get_storage

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
gc_observer = metrics.Observer(
    "gc_processor",
    buckets=[
        0.025,
        0.05,
        0.1,
        1.0,
        5.0,
        10.0,
        30.0,
        60.0,
        120.0,
        300.0,
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
        self.node = node
        self.publisher = IndexedPublisher()
        self.load_seqid()
        self.brain: Optional[Resource] = None

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)
        await self.publisher.initialize()
        await self.subscriber_initialize()

    async def finalize(self):
        await self.publisher.finalize()
        await self.subscriber_finalize()

        await self.storage.finalize()

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

        if indexing_settings.index_jetstream_auth is not None:
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

    async def set_resource(self, pb: IndexMessage) -> OpStatus:
        self.brain = await self.storage.get_indexing(pb)
        self.brain.shard_id = self.brain.resource.shard_id = pb.shard
        logger.info(
            f"Added {self.brain.resource.uuid} at {self.brain.shard_id} otx:{pb.txid}"
        )
        status = await self.writer.set_resource(self.brain)
        logger.info(f"...done")
        del self.brain
        self.brain = None
        return status

    async def delete_resource(self, pb: IndexMessage) -> OpStatus:
        logger.info(f"Deleting {pb.resource} otx:{pb.txid}")
        rid = ResourceID(uuid=pb.resource, shard_id=pb.shard)
        status = await self.writer.delete_resource(rid)
        logger.info(f"...done")
        return status

    @subscriber_observer.wrap()
    async def subscription_worker(self, msg: Msg):
        subject = msg.subject
        reply = msg.reply
        seqid = int(msg.reply.split(".")[5])
        if self.last_seqid and self.last_seqid >= seqid:
            logger.warning(
                f"Skipping already processed message. Msg seqid {seqid} vs Last seqid {self.last_seqid}"
            )
            await msg.ack()
            return

        pb = IndexMessage()
        pb.ParseFromString(msg.data)
        logger.info(
            "Message received",
            extra={
                "shard": pb.shard,
                "subject": subject,
                "reply": reply,
                "seqid": seqid,
                "storage_key": pb.storage_key,
            },
        )

        status: Optional[OpStatus] = None
        start = time.time()
        async with MessageProgressUpdater(
            msg, nats_consumer_settings.nats_ack_wait * 0.66
        ):
            try:
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
                    if (
                        pb.typemessage == TypeMessage.CREATION
                        and self.brain
                        and self.brain.HasField("metadata")
                    ):
                        # Hard fail if we have the correct data
                        await msg.nak()
                        raise grpc_error

            except IndexDataNotFound as storage_error:
                # This should never happen now.
                # Remove this block in the future once we're confident it's not needed.
                errors.capture_exception(storage_error)
                logger.warning(
                    "Error retrieving the indexing payload we do not block as that means its already deleted!"
                )
            except Exception as e:
                event_id = errors.capture_exception(e)
                logger.error(
                    f"An error on subscription_worker. Check sentry for more details. Event id: {event_id}"
                )
                await msg.nak()
                raise e

        try:
            await msg.ack()
            await self.publisher.indexed(pb)
            self.store_seqid(seqid)
        except Exception as e:  # pragma: no cover
            await msg.nak()
            errors.capture_exception(e)
            logger.error(
                f"An error on subscription_worker. Check sentry for more details."
            )
            raise e
        else:
            logger.info(
                "Message finished",
                extra={
                    "shard": pb.shard,
                    "storage_key": pb.storage_key,
                    "time": time.time() - start,
                },
            )

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


class IndexedPublisher:
    def __init__(self):
        self.pubsub = None

    async def initialize(self):
        self.pubsub = await get_pubsub()

    async def finalize(self):
        await self.pubsub.finalize()

    async def indexed(self, indexpb: IndexMessage):
        if not indexpb.HasField("partition"):
            logger.warning(f"Could not publish message without partition")
            return

        message = Notification(
            partition=int(indexpb.partition),
            seqid=indexpb.txid,
            uuid=indexpb.resource,
            kbid=indexpb.kbid,
            action=Notification.INDEXED,
        )

        await self.pubsub.publish(
            const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=indexpb.kbid),
            message.SerializeToString(),
        )
