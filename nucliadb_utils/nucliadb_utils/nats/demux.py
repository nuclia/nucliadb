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
from dataclasses import dataclass
from functools import partial, total_ordering
from typing import Any, Awaitable, Callable, Optional, Type

from nats.aio.client import Msg

from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils.nats.message_progress_updater import MessageProgressUpdater
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.signals import Signal

logger = logging.getLogger(__name__)


_Split = str
_Splitter = Callable[[Msg], tuple[_Split, Any]]
_Callback = Callable[[Any], Awaitable[bool]]


@total_ordering
@dataclass
class WorkUnit:
    work_id: str
    work: Any

    def __eq__(self, other: "WorkUnit"):
        return self.work_id == other.work_id

    def __lt__(self, other: "WorkUnit"):
        return self.work.__lt__(other.work)

    def __hash__(self):
        return self.work_id.__hash__()


@dataclass
class WorkOutcome:
    work_id: str
    success: Optional[bool] = None
    error: Optional[Exception] = None

    def __post_init__(self):
        if self.success is not None and self.error is not None:
            raise ValueError("Can't create a work outcome with success and error")
        elif self.success is None and self.error is None:
            raise ValueError("Can't create a work outcome without success or error")


work_unit_done = Signal(payload_model=WorkOutcome)


class NatsDemultiplexer:
    """Given a NATS consumer, allow concurrent message processing.

    Message processing will be done in a separate async task. Make sure
    `manual_ack` is set to True in NATS configuration to avoid JetStream
    autoacking the messages.



             (split X)
                 |                     asyncio.Queue
             /|  |                     --+--+--+--+
            / +--+-- work unit ----> ... |  |  |  |  <---- DemuxWorker
           /  |                        --+--+--+--+    (separate async task)
           |  +-----  ...                                    |
           |  |                                              |
    Msg ---+  +-----  ...                                    |
           |  |                                              +---> process_cb(work unit)
           |  +    ...                                       |      |
           \\ |                                              |      |        +--- ack
            | +-----  ...                                    |      |        |
            |\\                                              +------+---->---+
            |                                                                |
            +--- splitter                                                    +--- nak
    """

    def __init__(
        self,
        *,
        splitter: _Splitter,
        process_cb: _Callback,
        queue_klass: Type[asyncio.Queue] = asyncio.Queue,
    ):
        """A `splitter` function gets the original message and returns the split
        and a work unit. The work unit is added to a queue and a concurrent
        worker gets it and calls `cb`. The result is a boolean indicating if the
        message should be acked or nacked.

        If an exception is raised during any of this, the message is
        automatically nacked.

        Remember: if you use a priority queue, the work unit returned by
        `splitter` should be sortable in the desired order.

        """
        self.splitter = splitter
        self.process_cb = process_cb
        self.queue_klass = queue_klass
        self.workers: dict[str, "DemuxWorker"] = {}

    def handle_message_nowait(self, msg: Msg):
        def done_callback(task):
            if task.exception() is not None:
                error = task.exception()
                event_id = capture_exception(error)
                logger.error(
                    "Error while concurrently handling a NATS message. "
                    f"Check sentry for more details. Event id: {event_id}",
                    exc_info=True,
                )
                raise error

        task = asyncio.create_task(self.demux(msg))
        task.add_done_callback(done_callback)

    async def demux(self, msg: Msg):
        try:
            await self._demux(msg)
        except Exception as exc:
            logger.info("Exception on demux", exc_info=exc)
            await msg.nak()
            raise exc

    async def _demux(self, msg: Msg):
        async with MessageProgressUpdater(
            msg, nats_consumer_settings.nats_ack_wait * 0.66
        ):
            split, work = self.splitter(msg)

            seqid = int(msg.reply.split(".")[5])
            work_id = f"{split}:{seqid}"

            worker, created = self.get_or_create_worker(split)
            if created or (not created and not worker.working):
                # The second condition should never happen, the worker task finished
                # between us getting the worker and adding work. We'll create a new
                # task for it
                self.start_worker(split)

            work_unit = WorkUnit(work_id, work)

            await worker.add_work(work_unit)

            loop = asyncio.get_event_loop()
            wait_condition = loop.create_future()

            work_unit_done.add_listener(
                work_id,
                partial(
                    NatsDemultiplexer.on_work_unit_done,
                    work_id=work_id,
                    notifier=wait_condition,
                ),
            )

            work_outcome = await wait_condition
            if work_outcome.error:
                await msg.nak()
            else:
                if work_outcome.success:
                    await msg.ack()
                else:
                    await msg.nak()

    def get_or_create_worker(self, worker_id: str) -> tuple["DemuxWorker", bool]:
        new_worker = worker_id not in self.workers
        if new_worker:
            queue = self.queue_klass()
            worker = DemuxWorker(queue, self.process_cb)
            self.workers[worker_id] = worker
        else:
            worker = self.workers[worker_id]
        return (worker, new_worker)

    def start_worker(self, worker_id: str):
        worker = self.workers[worker_id]
        task = asyncio.create_task(worker.work_until_finish())
        task.add_done_callback(partial(self.demux_worker_done, worker_id))
        return task

    def demux_worker_done(self, worker_id: str, task: asyncio.Task):
        self.workers.pop(worker_id, None)

        # propagate errors
        if task.exception() is not None:
            raise task.exception()

    @staticmethod
    async def on_work_unit_done(
        work_id: str,
        notifier: asyncio.Future,
        payload: WorkOutcome,
    ):
        if payload.work_id == work_id:
            notifier.set_result(payload)


class DemuxWorker:
    def __init__(self, work_queue: asyncio.Queue, cb: _Callback):
        self.cb = cb
        self.work_queue = work_queue
        self.working = False
        self.current_work_ids: set[str] = set()

    async def add_work(self, work: Any):
        if work.work_id in self.current_work_ids:
            logger.warning(f"Trying to add already pending work unit: {work.work_id}")
            return
        await self.work_queue.put(work)

    async def work_until_finish(self):
        self.working = True
        while not self.work_queue.empty():
            work: WorkUnit = await self.work_queue.get()
            await self.do_work(work)
        self.working = False

    async def do_work(self, work: WorkUnit):
        try:
            success = await self.cb(work.work)
        except Exception as exc:
            event_id = capture_exception(exc)
            logger.error(
                "An error happened on nats demux worker. Check sentry for more details. "
                f"Event id: {event_id}",
                exc_info=exc,
            )
            await work_unit_done.dispatch(WorkOutcome(work_id=work.work_id, error=exc))
        else:
            await work_unit_done.dispatch(
                WorkOutcome(work_id=work.work_id, success=success)
            )
        finally:
            self.current_work_ids.discard(work.work_id)
