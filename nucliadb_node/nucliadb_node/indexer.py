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
import logging
import time
from functools import cached_property, partial, total_ordering
from typing import Optional

from grpc import StatusCode
from grpc.aio import AioRpcError
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
from nucliadb_utils.storages.storage import Storage
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

    def __new__(
        cls, *, index_message: IndexMessage, nats_msg: Msg, mpu: MessageProgressUpdater
    ):
        instance = super().__new__(cls)
        instance.index_message = index_message
        instance.nats_msg = nats_msg
        instance.mpu = mpu
        return instance

    @classmethod
    def from_msg(cls, msg: Msg) -> "WorkUnit":
        mpu = MessageProgressUpdater(msg, nats_consumer_settings.nats_ack_wait * 0.66)

        pb = IndexMessage()
        pb.ParseFromString(msg.data)

        return cls.__new__(cls, index_message=pb, nats_msg=msg, mpu=mpu)

    @property
    def seqid(self) -> int:
        return int(self.nats_msg.reply.split(".")[5])

    @property
    def shard_id(self) -> str:
        return self.index_message.shard

    @cached_property
    def _sortable_id(self) -> tuple[int, int]:
        # Priority based on message source and smaller seqid
        source_priority = self.priorities[self.index_message.source]
        return (source_priority, self.seqid)

    def __eq__(self, other) -> bool:
        if not isinstance(other, WorkUnit):
            return NotImplemented  # pragma: no cover
        return self._sortable_id.__eq__(other._sortable_id)

    def __lt__(self, other):
        if not isinstance(other, WorkUnit):
            return NotImplemented  # pragma: no cover
        return self._sortable_id.__lt__(other._sortable_id)


class ConcurrentShardIndexer:
    def __init__(self, writer: Writer, node_id: str):
        self.writer = writer
        self.node_id = node_id
        self.indexers: dict[str, tuple["PriorityIndexer", asyncio.Task]] = {}

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)

    async def finalize(self):
        tasks = []
        reverse_indexers = {}
        for shard_id, (_, task) in self.indexers.items():
            tasks.append(task)
            reverse_indexers[task] = shard_id

        if len(tasks):
            # REVIEW timeout. Is 10 a good timeout? Should we implement some
            # indexer finalize policy? Nak everything and close, for example?
            done, pending = await asyncio.wait(tasks, timeout=10)
            if pending:
                logger.error(
                    f"{len(pending)} pending tasks are still running after concurrent shard indexer finalizes!"
                )

            for task in done:
                error = task.exception()
                if error is not None:
                    shard_id = reverse_indexers[task]
                    logger.exception(
                        "Indexer task raised an error while finalizing it",
                        extra={
                            "shard": shard_id,
                        },
                        exc_info=error,
                    )

        self.indexers.clear()

    def index_message_soon(self, msg: Msg):
        work = WorkUnit.from_msg(msg)

        logger.info(
            "Index message is being enqueued",
            extra={
                "subject": msg.subject,
                "reply": msg.reply,
                "seqid": work.seqid,
                "kbid": work.index_message.kbid,
                "shard": work.shard_id,
                "storage_key": work.index_message.storage_key,
            },
        )

        self.push_work_to_indexer(work)
        work.mpu.start()
        CONCURRENT_INDEXERS_COUNT.set(
            len(self.indexers), labels=dict(node=self.node_id)
        )

    def push_work_to_indexer(self, work: WorkUnit):
        exists = work.shard_id in self.indexers
        if exists:
            indexer, _ = self.indexers[work.shard_id]
            indexer.index_soon(work)
        else:
            indexer = PriorityIndexer(writer=self.writer, storage=self.storage)
            indexer.index_soon(work)
            task = asyncio.create_task(indexer.work_until_finish())
            task.add_done_callback(partial(self._indexer_done_callback, work.shard_id))
            self.indexers[work.shard_id] = (indexer, task)

    def _indexer_done_callback(self, shard_id: str, task: asyncio.Task):
        error = task.exception()
        if error is not None:
            event_id = capture_exception(error)
            logger.error(
                f"Unexpected error on an indexer for shard {shard_id}. "
                f"Check sentry for more details. Event id: {event_id}",
                exc_info=error,
            )
            # TODO: bubble up error and restart pod?
            raise error

        indexer, _ = self.indexers[shard_id]
        # NOTE: there is an unusual situation where an indexer finishes but
        # new work is added before this done callback is executed. If
        # there's pending work on the indexer, we start a new task for it
        if indexer.has_pending_work():
            task = asyncio.create_task(indexer.work_until_finish())
            task.add_done_callback(partial(self._indexer_done_callback, shard_id))
            self.indexers[shard_id] = (indexer, task)
        else:
            self.indexers.pop(shard_id)
            CONCURRENT_INDEXERS_COUNT.set(
                len(self.indexers), labels=dict(node=self.node_id)
            )


class PriorityIndexer:
    def __init__(self, writer: Writer, storage: Storage):
        self.writer = writer
        self.storage = storage
        self.work_queue: asyncio.PriorityQueue[WorkUnit] = asyncio.PriorityQueue()
        self.working = False
        self.error = False
        self.last_worked_seqid: Optional[int] = None
        self.back_to_work = asyncio.Event()

    def has_pending_work(self) -> bool:
        return self.work_queue.qsize() > 0

    def index_soon(self, work: WorkUnit):
        self.work_queue.put_nowait(work)

        if self.error:
            if self.last_worked_seqid is None:
                # this should never happen, but if last_worked_seqid is never
                # set, we should resume work, as we'll never find the culprit
                self.back_to_work.set()
                self.error = False
            elif work.seqid == self.last_worked_seqid:
                # if we find the message that generated the error, we can go
                # back to work
                self.back_to_work.set()
                self.error = False

    async def work_until_finish(self):
        self.working = True
        while self.working:
            try:
                while not self.work_queue.empty():
                    work = self.work_queue.get_nowait()
                    self.last_worked_seqid = work.seqid
                    await self._do_work(work)
                self.working = False

            except Exception as exc:
                # if an exception occurred, we can't longer ensure proper
                # ordering for this queue. We flush the queue and wait for the
                # messages to be redelivered

                self.error = True
                self.report_exception(exc, work)

                # remove all elements without yielding the event loop
                logger.info(f"Flushing {self.work_queue.qsize()} messages from indexer")
                pending_work = []
                while not self.work_queue.empty():
                    pending_work.append(self.work_queue.get_nowait())

                # Wait for some time before nak'ing the messages to avoid
                # flooding stdout with logs in case of an unhandled error.
                await asyncio.sleep(1)

                await work.nats_msg.nak()
                await work.mpu.end()

                for w in pending_work:
                    logger.debug(
                        "Flushing message from indexer",
                        extra={
                            "seqid": w.seqid,
                            "kbid": w.index_message.kbid,
                            "shard": w.index_message.shard,
                            "storage_key": w.index_message.storage_key,
                        },
                    )
                    await w.nats_msg.nak()
                    await w.mpu.end()

                await self.back_to_work.wait()
                self.back_to_work.clear()

    def report_exception(self, exc: Exception, work: WorkUnit):
        if isinstance(exc, AioRpcError):
            grpc_error = exc
            if grpc_error.code() == StatusCode.UNAVAILABLE:
                level = logging.WARNING
                message = (
                    f"Writer node is unavailable. All messages for indexer {work.shard_id} "
                    "will be flushed and retried in a while"
                )
            else:
                event_id = capture_exception(exc)
                level = logging.ERROR
                message = (
                    "A gRPC error happened on an indexer, all messages for its queue will be flushed. "
                    f"Check sentry for more details. Event id: {event_id}"
                )
        else:
            event_id = capture_exception(exc)
            level = logging.ERROR
            message = (
                "An error happened on an indexer, all messages for its queue will be flushed. "
                f"Check sentry for more details. Event id: {event_id}"
            )

        logger.log(
            level,
            message,
            exc_info=exc,
            extra={
                "seqid": work.seqid,
                "kbid": work.index_message.kbid,
                "shard": work.index_message.shard,
                "storage_key": work.index_message.storage_key,
            },
        )

    @indexer_observer.wrap()
    async def _do_work(self, work: WorkUnit):
        _extra = {
            "seqid": work.seqid,
            "kbid": work.index_message.kbid,
            "shard": work.index_message.shard,
            "storage_key": work.index_message.storage_key,
        }
        start = time.time()
        logger.info(
            "Working on message for shard {shard_id} (pending work: {pending_work})".format(
                shard_id=work.index_message.shard, pending_work=self.work_queue.qsize()
            ),
            extra=_extra,
        )
        await self._index_message(work.index_message)
        await work.nats_msg.ack()
        await work.mpu.end()
        await signals.successful_indexing.dispatch(
            SuccessfulIndexingPayload(
                seqid=work.seqid, index_message=work.index_message
            )
        )
        logger.info(
            "Message indexing finished",
            extra={
                **_extra,
                "indexing_time": time.time() - start,
            },
        )

    async def _index_message(self, pb: IndexMessage):
        status = None
        if pb.typemessage == TypeMessage.CREATION:
            status = await self._set_resource(pb)
        elif pb.typemessage == TypeMessage.DELETION:
            status = await self._delete_resource(pb)
        if status is not None and status.status != OpStatus.Status.OK:
            if (
                status.status == OpStatus.Status.ERROR
                and "Shard not found" in status.detail
            ):
                logger.warning(
                    f"Shard does not exist {pb.shard}. This message will be ignored",
                    extra={
                        "kbid": pb.kbid,
                        "shard": pb.shard,
                        "rid": pb.resource,
                        "storage_key": pb.storage_key,
                    },
                )
                return
            raise IndexNodeError(status.detail)

    async def _set_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
        brain = await self.storage.get_indexing(pb)
        shard_id = pb.shard
        rid = brain.resource.uuid
        brain.shard_id = brain.resource.shard_id = shard_id
        _extra = {
            "kbid": pb.kbid,
            "shard": pb.shard,
            "rid": rid,
            "storage_key": pb.storage_key,
        }

        logger.debug(f"Adding {rid} at {shard_id} otx:{pb.txid}", extra=_extra)
        try:
            status = await self.writer.set_resource(brain)

        except AioRpcError as grpc_error:
            if grpc_error.code() == StatusCode.NOT_FOUND:
                logger.error(
                    f"Shard does not exist {pb.shard}. This message will be ignored",
                    extra=_extra,
                )
            else:
                # REVIEW: we should always have metadata and the writer node
                # should handle this in a better way than a panic
                if brain.HasField("metadata"):
                    # Hard fail if we have the correct data
                    raise grpc_error
                else:
                    event_id = errors.capture_exception(grpc_error)
                    logger.error(
                        "Error on indexer worker trying to set a resource without metadata. "
                        "This message won't be retried. Check sentry for more details. "
                        f"Event id: {event_id}",
                        extra=_extra,
                    )
            return None

        else:
            logger.debug(
                f"...done (Added {rid} at {shard_id} otx:{pb.txid})", extra=_extra
            )
            return status

    async def _delete_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
        shard_id = pb.shard
        rid = pb.resource
        resource = ResourceID(uuid=rid, shard_id=shard_id)
        _extra = {
            "kbid": pb.kbid,
            "shard": pb.shard,
            "rid": rid,
            "storage_key": pb.storage_key,
        }

        logger.debug(f"Deleting {rid} in {shard_id} otx:{pb.txid}", extra=_extra)
        try:
            status = await self.writer.delete_resource(resource)

        except AioRpcError as grpc_error:
            if grpc_error.code() == StatusCode.NOT_FOUND:
                logger.error(
                    f"Shard does not exist {pb.shard}. This message will be ignored",
                    extra=_extra,
                )
            else:
                raise grpc_error
            return None

        else:
            logger.debug(
                f"...done (Deleted {rid} in {shard_id} otx:{pb.txid})", extra=_extra
            )
            return status
