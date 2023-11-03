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
import time
from abc import ABC
from typing import List, Optional

import nats
from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore
from nats.aio.client import Msg
from nats.aio.subscription import Subscription
from nats.js.errors import NotFoundError as StreamNotFoundError
from nucliadb_protos.noderesources_pb2 import Resource, ResourceID
from nucliadb_protos.nodewriter_pb2 import (
    IndexMessage,
    IndexMessageSource,
    OpStatus,
    TypeMessage,
)
from nucliadb_protos.writer_pb2 import Notification

from nucliadb_node import SERVICE_NAME, logger
from nucliadb_node.coordination import Priority, PriorityLock, ShardIndexingCoordinator
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

SHARD_INDEX_COORDINATOR = ShardIndexingCoordinator(PriorityLock)


class ShardManager:
    schedule_delay_seconds = 30.0

    def __init__(self, shard_id: str, writer: Writer, gc_lock: asyncio.Semaphore):
        self.lock = asyncio.Lock()  # write lock for this shard
        self.target_gc_resources = settings.max_resources_before_gc

        self._shard_id = shard_id
        self._writer = writer

        self._gc_lock = gc_lock  # global lock so we only gc one shard at a time
        self._change_count = 0
        self._gc_schedule_timer: Optional[asyncio.TimerHandle] = None
        self._gc_task: Optional[asyncio.Task] = None

    def shard_changed_event(self, delay: Optional[float] = None) -> None:
        """
        Signal shard has changed and should be garbage collected at some point
        """
        if self._gc_task is not None and not self._gc_task.done():
            # already running or scheduled
            return

        self._change_count += 1
        if (
            self._gc_schedule_timer is not None
            and not self._gc_schedule_timer.cancelled()
        ):
            self._gc_schedule_timer.cancel()

        if self._change_count >= self.target_gc_resources:
            # need to force running it now
            self._schedule_gc()
        else:
            # run it soon
            if delay is None:
                delay = self.schedule_delay_seconds
            self._gc_schedule_timer = asyncio.get_event_loop().call_later(
                delay, self._schedule_gc
            )

    def _schedule_gc(self) -> None:
        self._gc_task = asyncio.create_task(self.gc())

    async def gc(self):
        async with self._gc_lock, self.lock:
            logger.info("Running garbage collection", extra={"shard": self._shard_id})
            self._change_count = 0
            try:
                with gc_observer():
                    # NOTE: garbage collector may not run if the shard is busy. We currently don't do anything to retry.
                    await self._writer.garbage_collector(self._shard_id)
                    logger.info(
                        "Garbage collection finished", extra={"shard": self._shard_id}
                    )
            except Exception:
                logger.exception(
                    "Could not garbage collect", extra={"shard": self._shard_id}
                )


class Worker(ABC):
    """A `Worker` pulls index messages from a nats queue and index them to the
    corresponding nodes

    """

    subscriptions: List[Subscription]
    storage: Storage

    worker_name = "base"
    sources = {
        IndexMessageSource.PROCESSOR: Priority.LOW,
        IndexMessageSource.WRITER: Priority.HIGH,
    }

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
        self.brain: Optional[Resource] = None

        self.shard_managers: dict[str, ShardManager] = {}
        # right now, only allow one gc at a time but
        # can be expanded to allow multiple if we want with a semaphore
        self.gc_lock = asyncio.Semaphore(1)

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)
        await self.publisher.initialize()
        await self.subscriber_initialize()
        await self.garbage_collect_all()

    async def finalize(self):
        await self.publisher.finalize()
        await self.subscriber_finalize()

        await self.storage.finalize()

    async def disconnected_cb(self):
        logger.info(f"[worker:{self.worker_name}] Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect
        logger.warning(
            f"[worker:{self.worker_name}] Got reconnected to NATS {self.nc.connected_url}. Attempting reconnect"
        )
        await self.drain_subscriptions()
        await self.subscribe()

    async def error_cb(self, e):
        errors.capture_exception(e)
        logger.error(
            f"[worker:{self.worker_name}] There was an error on the worker, check sentry: {e}",
            exc_info=True,
        )

    async def closed_cb(self):
        logger.info(f"[worker:{self.worker_name}] Connection is closed on NATS")

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
        logger.info(
            f"[worker:{self.worker_name}] Nats: Connected to {indexing_settings.index_jetstream_servers}"
        )
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

    def get_shard_manager(self, shard_id: str) -> ShardManager:
        if shard_id not in self.shard_managers:
            self.shard_managers[shard_id] = ShardManager(
                shard_id, self.writer, self.gc_lock
            )
        return self.shard_managers[shard_id]

    async def garbage_collect_all(self) -> None:
        """
        Schedule garbage collection for all shards on startup
        """
        for idx, shard in enumerate((await self.writer.shards()).ids):
            sm = self.get_shard_manager(shard.id)
            sm.shard_changed_event(idx * 0.01)

    async def set_resource(self, pb: IndexMessage) -> OpStatus:
        self.brain = await self.storage.get_indexing(pb)
        self.brain.shard_id = self.brain.resource.shard_id = pb.shard
        logger.info(
            f"[worker:{self.worker_name}] Added {self.brain.resource.uuid} at "
            f"{self.brain.shard_id} otx:{pb.txid}"
        )
        status = await self.writer.set_resource(self.brain)
        logger.info(f"[worker:{self.worker_name}] ...done")
        del self.brain
        self.brain = None
        return status

    async def delete_resource(self, pb: IndexMessage) -> OpStatus:
        logger.info(f"[worker:{self.worker_name}] Deleting {pb.resource} otx:{pb.txid}")
        rid = ResourceID(uuid=pb.resource, shard_id=pb.shard)
        status = await self.writer.delete_resource(rid)
        logger.info(f"[worker:{self.worker_name}] ...done")
        return status

    @subscriber_observer.wrap()
    async def subscription_worker(self, msg: Msg):
        subject = msg.subject
        reply = msg.reply
        seqid = int(msg.reply.split(".")[5])

        pb = IndexMessage()
        pb.ParseFromString(msg.data)
        if not self.interested_source(pb.source):
            await msg.ack()
            logger.debug(
                f"[worker:{self.worker_name}] skipping not relevant message",
                extra={
                    "shard": pb.shard,
                    "subject": subject,
                    "reply": reply,
                    "seqid": seqid,
                    "storage_key": pb.storage_key,
                },
            )
            return

        logger.info(
            f"[worker:{self.worker_name}] Message received",
            extra={
                "shard": pb.shard,
                "subject": subject,
                "reply": reply,
                "seqid": seqid,
                "storage_key": pb.storage_key,
            },
        )

        status: Optional[OpStatus] = None
        sm = self.get_shard_manager(pb.shard)
        start = time.time()
        async with MessageProgressUpdater(
            msg, nats_consumer_settings.nats_ack_wait * 0.66
        ), sm.lock:
            try:
                await SHARD_INDEX_COORDINATOR.request_shard(
                    pb.shard, priority=self.source_priority(pb.source)
                )
                indexing_start = time.time()

                if pb.typemessage == TypeMessage.CREATION:
                    status = await self.set_resource(pb)
                elif pb.typemessage == TypeMessage.DELETION:
                    status = await self.delete_resource(pb)
                if status:
                    self.reader.update(pb.shard, status)
                sm.shard_changed_event()
            except AioRpcError as grpc_error:
                if grpc_error.code() == StatusCode.NOT_FOUND:
                    logger.error(
                        f"[worker:{self.worker_name}] Shard does not exist {pb.shard}"
                    )
                else:
                    event_id = errors.capture_exception(grpc_error)
                    logger.error(
                        f"[worker:{self.worker_name}] An error on subscription_worker. Check sentry for "
                        f"more details. Event id: {event_id}"
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
                    f"[worker:{self.worker_name}] Error retrieving the indexing payload we do not "
                    "block as that means its already deleted!"
                )
            except Exception as e:
                event_id = errors.capture_exception(e)
                logger.error(
                    f"[worker:{self.worker_name}] An error on subscription_worker. Check sentry for "
                    f"more details. Event id: {event_id}"
                )
                await msg.nak()
                raise e
            finally:
                await SHARD_INDEX_COORDINATOR.release_shard(pb.shard)

        try:
            await msg.ack()
            await self.publisher.indexed(pb)
        except Exception as e:  # pragma: no cover
            await msg.nak()
            errors.capture_exception(e)
            logger.error(
                f"[worker:{self.worker_name}] An error on subscription_worker. Check sentry for more details."
            )
            raise e
        else:
            now = time.time()
            logger.info(
                f"[worker:{self.worker_name}] Message finished",
                extra={
                    "shard": pb.shard,
                    "storage_key": pb.storage_key,
                    "time": now - start,
                    "indexing_time": now - indexing_start,
                },
            )

    async def subscribe(self):
        try:
            await self.js.stream_info(const.Streams.INDEX.name)
        except StreamNotFoundError:
            logger.info(f"[worker:{self.worker_name}] Creating stream")
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
            queue=const.Streams.INDEX.group.format(
                node=self.node, worker=self.worker_name
            ),
            stream=const.Streams.INDEX.name,
            flow_control=True,
            cb=self.subscription_worker,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.NEW,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_ack_pending=nats_consumer_settings.nats_max_ack_pending,
                max_deliver=nats_consumer_settings.nats_max_deliver,
                ack_wait=nats_consumer_settings.nats_ack_wait,
                idle_heartbeat=nats_consumer_settings.nats_idle_heartbeat,
            ),
        )
        self.subscriptions.append(res)
        logger.info(
            f"[worker:{self.worker_name}] Subscribed to {subject} on stream {const.Streams.INDEX.name}"
        )

    @classmethod
    def interested_source(cls, source: IndexMessageSource.ValueType) -> bool:
        return source in cls.sources

    @classmethod
    def source_priority(cls, source: IndexMessageSource.ValueType) -> Priority:
        return cls.sources[source]


class ProcessorWorker(Worker):
    """
    Pull worker for messages coming from the processor
    """

    worker_name = "processor"
    sources = {
        IndexMessageSource.PROCESSOR: Priority.LOW,
    }


class WriterWorker(Worker):
    """
    Pull worker for messages coming from the writer
    """

    worker_name = "writer"
    sources = {
        IndexMessageSource.WRITER: Priority.HIGH,
    }


class IndexedPublisher:
    def __init__(self):
        self.pubsub = None

    async def initialize(self):
        self.pubsub = await get_pubsub()

    async def finalize(self):
        await self.pubsub.finalize()

    async def indexed(self, indexpb: IndexMessage):
        if not indexpb.HasField("partition"):
            logger.warning("Could not publish message without partition")
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
