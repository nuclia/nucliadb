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
from dataclasses import dataclass
from functools import partial, total_ordering
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
from nucliadb_node.dispatcher import ListenerPriority
from nucliadb_node.signals import FailedIndexingPayload, SuccessfulIndexingPayload
from nucliadb_node.writer import Writer
from nucliadb_telemetry import errors
from nucliadb_utils.nats import MessageProgressUpdater
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage

# Messages coming from processor take loner to index, so we want to prioritize
# small messages coming from the writer (user creations/updates/deletes)
PRIORITIES = {
    IndexMessageSource.PROCESSOR: 0,
    IndexMessageSource.WRITER: 1,
}


@total_ordering
@dataclass
class WorkUnit:
    seqid: int
    message: IndexMessage
    nats_msg: Msg

    def priority(self) -> int:
        return PRIORITIES[self.message.source]

    def __eq__(self, other):
        return self.priority().__eq__(other.priority())

    def __lt__(self, other):
        return self.priority().__lt__(other.priority())

    def __hash__(self):
        return self.seqid.__hash__()


class ConcurrentShardIndexer:
    def __init__(self, writer: Writer):
        self.workers: dict[str, IndexerWorker] = {}
        self.writer = writer

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)

    async def finalize(self):
        pass

    async def index_message(self, msg: Msg):
        # Ensure any error triggers a nak to NATS
        try:
            await self._index_message(msg)
        except Exception as exc:
            await msg.nak()
            raise exc

    async def _index_message(self, msg: Msg):
        """This function will wait until the message is actually processed. If
        concurrency is desired, run it in another task.

        """
        subject = msg.subject
        reply = msg.reply
        seqid = int(msg.reply.split(".")[5])

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

        start = time.time()
        async with MessageProgressUpdater(
            msg, nats_consumer_settings.nats_ack_wait * 0.66
        ):
            shard_id = pb.shard
            work = WorkUnit(message=pb, seqid=seqid, nats_msg=msg)

            worker, created = self.get_or_create_worker(shard_id)
            worker.add_work(work)

            if created or (not created and not worker.working):
                # The second condition should never happen, the worker task finished
                # between us getting the worker and adding work. We'll create a new
                # task for it
                _ = self.start_worker(worker)

            listener_id = f"indexer-{shard_id}-{seqid}"
            loop = asyncio.get_event_loop()
            wait_condition = loop.create_future()

            signals.successful_indexing.add_listener(
                listener_id,
                partial(
                    ConcurrentShardIndexer.on_successful_indexing,
                    interesting_seqid=seqid,
                    notifier=wait_condition,
                ),
                priority=ListenerPriority.CRITICAL,
            )
            signals.failed_indexing.add_listener(
                listener_id,
                partial(
                    ConcurrentShardIndexer.on_failed_indexing,
                    interesting_seqid=seqid,
                    notifier=wait_condition,
                ),
                priority=ListenerPriority.CRITICAL,
            )

            # wait until our message is indexed
            success, error = await wait_condition

            signals.successful_indexing.remove_listener(listener_id)
            signals.failed_indexing.remove_listener(listener_id)

            if success:
                # TODO? seqid management
                await msg.ack()
                logger.info(
                    "Message finished",
                    extra={
                        "shard": shard_id,
                        "storage_key": pb.storage_key,
                        "time": time.time() - start,
                    },
                )
            else:
                await msg.nak()
                if error is not None:
                    raise error

    def get_or_create_worker(self, shard_id: str) -> tuple["IndexerWorker", bool]:
        new_worker = shard_id not in self.workers
        if new_worker:
            worker = IndexerWorker(self.writer, self.storage)
            self.workers[shard_id] = worker
        else:
            worker = self.workers[shard_id]
        return (worker, new_worker)

    def start_worker(self, worker: "IndexerWorker"):
        def indexer_task_done(task):
            self.clean_finished_workers()

            if task.exception() is not None:
                raise task.exception()

        task = asyncio.create_task(worker.work_until_finish())
        task.add_done_callback(indexer_task_done)
        return task

    def clean_finished_workers(self):
        finished = set()
        for wid, worker in self.workers.items():
            if worker.working:
                finished.add(wid)
        for wid in finished:
            self.workers.pop(wid)

    @staticmethod
    async def on_successful_indexing(
        interesting_seqid: int,
        notifier: asyncio.Future,
        payload: SuccessfulIndexingPayload,
    ):
        if payload.seqid == interesting_seqid:
            notifier.set_result((True, None))

    @staticmethod
    async def on_failed_indexing(
        interesting_seqid: int, notifier: asyncio.Future, payload: FailedIndexingPayload
    ):
        if payload.seqid == interesting_seqid:
            notifier.set_result((False, payload.error))


class IndexerWorker:
    def __init__(self, writer: Writer, storage: Storage):
        self.work_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.writer = writer
        self.storage = storage
        self.working = False

    def add_work(self, work: WorkUnit):
        self.work_queue.put_nowait(work)

    async def work_until_finish(self):
        self.working = True
        while not self.work_queue.empty():
            work = await self.work_queue.get()

            try:
                if work.message.typemessage == TypeMessage.CREATION:
                    await self.set_resource(work.message)

                elif work.message.typemessage == TypeMessage.DELETION:
                    await self.delete_resource(work.message)

                else:
                    logger.warning(
                        f"Unknown type message {work.message.typemessage}",
                        extra={
                            "seqid": work.seqid,
                            "shard": work.index_message.shard,
                        },
                    )

            except Exception as exc:
                event_id = errors.capture_exception(exc)
                logger.error(
                    "An unknown error occurred on indexer worker. "
                    f"Check sentry for more details. Event id: {event_id}"
                )
                payload = FailedIndexingPayload(seqid=work.seqid, error=exc)
                await signals.failed_indexing.dispatch(payload)

            else:
                payload = SuccessfulIndexingPayload(
                    seqid=work.seqid,
                    index_message=work.message,
                )
                await signals.successful_indexing.dispatch(payload)

        self.working = False

    async def set_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
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

    async def delete_resource(self, pb: IndexMessage) -> Optional[OpStatus]:
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
