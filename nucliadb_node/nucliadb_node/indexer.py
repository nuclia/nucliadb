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
from functools import cached_property, partial, total_ordering
from typing import Optional

from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore
from nats.aio.client import Msg
from nucliadb_protos.noderesources_pb2 import ResourceID
from nucliadb_protos.nodewriter_pb2 import (
    IndexMessage,
    IndexMessageSource,
    OpStatus,
    TypeMessage,
)

from nucliadb_node import SERVICE_NAME, logger, signals
from nucliadb_node.signals import SuccessfulIndexingPayload
from nucliadb_node.writer import Writer
from nucliadb_telemetry import errors, metrics
from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils.nats import MessageProgressUpdater
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.utilities import get_storage

CONCURRENT_INDEXERS_COUNT = metrics.Gauge(
    "nucliadb_concurrent_indexers_count", labels={"node": ""}
)
indexer_observer = metrics.Observer(
    "nucliadb_message_indexing",
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


class IndexNodeError(Exception):
    pass


@total_ordering
class WorkUnit:
    # Messages coming from processor take longer to index, so we want to
    # prioritize small messages coming from the writer (user
    # creations/updates/deletes).
    #
    # Priority order: lower values first
    priorities = {
        IndexMessageSource.WRITER: 0,
        IndexMessageSource.PROCESSOR: 1,
    }

    index_message: IndexMessage
    nats_msg: Msg
    mpu: MessageProgressUpdater

    def __init__(self, *args, **kwargs):
        raise Exception("__init__ method not allowed. Use a from_* method instead")

    @classmethod
    def __new__(
        cls, *, index_messsage: IndexMessage, nats_msg: Msg, mpu: MessageProgressUpdater
    ):
        instance = super().__new__(cls)
        instance.index_message = index_messsage
        instance.nats_msg = nats_msg
        instance.mpu = mpu
        return instance

    @classmethod
    def from_msg(cls, msg: Msg) -> "WorkUnit":
        mpu = MessageProgressUpdater(msg, nats_consumer_settings.nats_ack_wait * 0.66)

        pb = IndexMessage()
        pb.ParseFromString(msg.data)

        return cls.__new__(index_messsage=pb, nats_msg=msg, mpu=mpu)

    @property
    def seqid(self) -> int:
        return int(self.nats_msg.reply.split(".")[5])

    @property
    def node_id(self) -> str:
        return self.index_message.node

    @property
    def shard_id(self) -> str:
        return self.index_message.shard

    def __eq__(self, other) -> bool:
        if not isinstance(other, WorkUnit):
            return NotImplemented
        return self._priority_id.__eq__(other._priority_id)

    @cached_property
    def _priority_id(self) -> tuple[int, int]:
        # Priority based on message source and smaller seqid
        source_priority = self.priorities[self.index_message.source]
        return (source_priority, self.seqid)

    def __lt__(self, other):
        if not isinstance(other, WorkUnit):
            return NotImplemented
        return self._priority_id.__lt__(other._priority_id)


class ConcurrentShardIndexer:
    def __init__(self, writer: Writer):
        self.writer = writer
        self.workers: dict[str, PriorityIndexer] = {}

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)

    async def finalize(self):
        for indexer in self.workers.values():
            await indexer.finalize()
        self.workers.clear()

    async def index_message_soon(self, msg: Msg):
        work = WorkUnit.from_msg(msg)
        await work.mpu.start()

        logger.info(
            f"Index message for shard {work.shard_id} is being enqueued",
            extra={
                "shard": work.shard_id,
                "subject": msg.subject,
                "reply": msg.reply,
                "seqid": work.seqid,
                "storage_key": work.index_message.storage_key,
            },
        )

        indexer, created = await self.get_or_create_indexer(work)
        await indexer.index_soon(work)
        await self.clean_idle_indexers()
        CONCURRENT_INDEXERS_COUNT.set(len(self.workers), labels=dict(node=work.node_id))

    async def get_or_create_indexer(
        self, work: WorkUnit
    ) -> tuple["PriorityIndexer", bool]:
        create = work.shard_id not in self.workers
        if create:
            indexer = PriorityIndexer(self.writer)
            await indexer.initialize()
            self.workers[work.shard_id] = indexer
        else:
            indexer = self.workers[work.shard_id]
        return (indexer, create)

    async def clean_idle_indexers(self):
        remove = set()
        for shard_id, indexer in self.workers.items():
            if not indexer.working:
                remove.add(shard_id)
        for shard_id in remove:
            indexer = self.workers.pop(shard_id)
            await indexer.finalize()


class PriorityIndexer:
    def __init__(self, writer: Writer):
        self.writer = writer
        self.work_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.task: Optional[asyncio.Task] = None

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)

    async def finalize(self):
        if self.task is not None and not self.task.done():
            try:
                await self.task
            except Exception as exc:
                logger.warning(
                    "Indexing task raised an error while finalizing indexer",
                    exc_info=exc,
                )

    async def index_soon(self, work: WorkUnit):
        await self.work_queue.put(work)

        if self.task is None:
            task = asyncio.create_task(self._work_until_finish())
            task.add_done_callback(partial(PriorityIndexer._done_callback, self))
            self.task = task

    @property
    def working(self) -> bool:
        return self.task is not None

    async def _work_until_finish(self):
        try:
            while not self.work_queue.empty():
                work = await self.work_queue.get()
                await self._do_work(work)
        except Exception as exc:
            # if an exception occurred, we can't longer ensure proper ordering
            # for this queue. We flush the queue and wait for the messages to be
            # redelivered
            event_id = capture_exception(exc)
            logger.error(
                "An error happened on indexer, all messages for this queue will be flushed. "
                f"Check sentry for more details. Event id: {event_id}",
                extra={
                    "seqid": work.seqid,
                    "shard": work.index_message.shard,
                    "storage_key": work.index_message.storage_key,
                },
                exc_info=exc,
            )
            while not self.work_queue.empty():
                work = await self.work_queue.get()
                await work.nats_msg.nak()

    def _done_callback(self, task: asyncio.Task):
        self.task = None

        # propagate errors
        if task.exception() is not None:
            raise task.exception()  # type: ignore

    @indexer_observer.wrap()
    async def _do_work(self, work: WorkUnit):
        start = time.time()
        logger.info(
            f"Working on message for shard {work.index_message.shard} (seqid={work.seqid})",
            extra={
                "seqid": work.seqid,
                "shard": work.index_message.shard,
                "storage_key": work.index_message.storage_key,
            },
        )

        try:
            await self._index_message(work.index_message)
        except Exception as exc:
            raise exc
        else:
            await work.nats_msg.ack()

            await signals.successful_indexing.dispatch(
                SuccessfulIndexingPayload(
                    seqid=work.seqid, index_message=work.index_message
                )
            )
            logger.info(
                "Message indexing finished",
                extra={
                    "seqid": work.seqid,
                    "shard": work.index_message.shard,
                    "storage_key": work.index_message.storage_key,
                    "time": time.time() - start,
                },
            )
        finally:
            await work.mpu.end()

    async def _index_message(self, pb: IndexMessage):
        status = None
        if pb.typemessage == TypeMessage.CREATION:
            status = await self._set_resource(pb)
        elif pb.typemessage == TypeMessage.DELETION:
            status = await self._delete_resource(pb)

        if status is not None and status.status != OpStatus.Status.OK:
            raise IndexNodeError(status.detail)

    async def _set_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
        brain = await self.storage.get_indexing(pb)
        shard_id = pb.shard
        rid = brain.resource.uuid
        brain.shard_id = brain.resource.shard_id = shard_id

        logger.info(f"Adding {rid} at {shard_id} otx:{pb.txid}")
        try:
            status = await self.writer.set_resource(brain)

        except AioRpcError as grpc_error:
            if grpc_error.code() == StatusCode.NOT_FOUND:
                logger.error(f"Shard does not exist {pb.shard}")
            else:
                event_id = errors.capture_exception(grpc_error)
                logger.error(
                    "An error ocurred on indexer worker while setting a resource. "
                    f"Check sentry for more details. Event id: {event_id}"
                )
                if brain.HasField("metadata"):
                    # Hard fail if we have the correct data
                    raise grpc_error
            return None

        else:
            logger.info(f"...done (Added {rid} at {shard_id} otx:{pb.txid})")
            return status

    async def _delete_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
        shard_id = pb.shard
        rid = pb.resource
        resource = ResourceID(uuid=rid, shard_id=shard_id)

        logger.info(f"Deleting {rid} in {shard_id} otx:{pb.txid}")
        try:
            status = await self.writer.delete_resource(resource)

        except AioRpcError as grpc_error:
            if grpc_error.code() == StatusCode.NOT_FOUND:
                logger.error(f"Shard does not exist {pb.shard}")
            else:
                event_id = errors.capture_exception(grpc_error)
                logger.error(
                    "An error ocurred on indexer worker while deleting a resource. "
                    f"Check sentry for more details. Event id: {event_id}"
                )
            return None

        else:
            logger.info(f"...done (Deleted {rid} in {shard_id} otx:{pb.txid})")
            return status
