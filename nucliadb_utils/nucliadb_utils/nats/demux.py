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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import partial, total_ordering
from typing import Any, Awaitable, Callable, Optional, Type

from nats.aio.client import Msg

from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils.nats.message_progress_updater import MessageProgressUpdater
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.signals import Signal

logger = logging.getLogger(__name__)


Split = str
DemuxProcessorWork = Any


class DemuxProcessor(ABC):
    """Interface for NatsDemultiplexer internal processing."""

    @abstractmethod
    def splitter(self, msg: Msg) -> tuple[Split, DemuxProcessorWork]:
        """Decide in which split this message should go and return an appropiate
        work unit from it.

        NOTE: if work units will be added in an ordered data structure, the
        return value should be sortable in the desired order.

        """
        ...

    @abstractmethod
    async def process(self, work: DemuxProcessorWork) -> bool:
        """Process a work unit and return a boolean indicating processing
        success or failure.

        """
        ...


@total_ordering
@dataclass
class WorkUnit:
    work_id: str
    work: DemuxProcessorWork

    def __eq__(self, other):
        # A work unit is the same if we assign them the same id
        if not isinstance(other, WorkUnit):
            return NotImplemented
        return self.work_id == other.work_id

    def __lt__(self, other):
        # Rely on DemuxProcessorWork implementing partial ordering
        if not isinstance(other, WorkUnit):
            return NotImplemented
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

    NatsDemultiplexer relies on a DemuxProcessor. It's `splitter` function will
    be used to decide the split and the resulting work unit will be added into a
    queue.

    Concurrent workers will get work from their respective queues and use
    `DemuxProcessor.process` to process it. Depending on the result, the message
    will be acked or nacked to NATS.

    If an exception is raised during any of this process, the message will be
    automatically nacked.

    NOTE: Message processing will be done in a separate async task. Make sure
    `manual_ack` is set to True in NATS configuration to avoid JetStream
    autoacking the messages.


             (split X)
                 |                     asyncio.Queue
             /|  |                     --+--+--+--+
            / +--+-- work unit ----> ... |  |  |  |  <---- DemuxWorker
           /  |                        --+--+--+--+    (separate async task)
           |  +-----  ...                                    |
           |  |                                              | work
    Msg ---+  +-----  ...                                    |
           |  |                                              +---> DemuxProcessor.process
           |  +    ...                                       |      |
           \\ |                                              |      |        +--- ack
            | +-----  ...                                    |      |        |
            |\\                                              +------+---->---+
            |                                                                |
            +--- DemuxProcessor.splitter                                     +--- nak

    """

    def __init__(
        self,
        *,
        processor: DemuxProcessor,
        queue_klass: Type[asyncio.Queue] = asyncio.Queue,
    ):
        self.processor = processor
        self.queue_klass = queue_klass
        self.workers: dict[str, "DemuxWorker"] = {}
        self.working_on: set[str] = set()

    def handle_message_nowait(self, msg: Msg):
        """Eventually process `msg` asynchronously and ack/nak it when finished."""
        subject = msg.subject
        reply = msg.reply
        seqid = int(msg.reply.split(".")[5])
        logger.debug(
            "Message will be soon demultiplexed and processed",
            extra={"subject": subject, "reply": reply, "seqid": seqid},
        )
        asyncio.create_task(self.safe_demux(msg))

    async def safe_demux(self, msg: Msg):
        try:
            await self.demux(msg)
        except Exception as exc:
            subject = msg.subject
            reply = msg.reply
            seqid = int(msg.reply.split(".")[5])

            event_id = capture_exception(exc)
            logger.error(
                "Error while concurrently handling a NATS message. "
                f"Check sentry for more details. Event id: {event_id}",
                extra={
                    "subject": subject,
                    "reply": reply,
                    "seqid": seqid,
                },
                exc_info=exc,
            )

            await msg.nak()
            raise exc

    async def demux(self, msg: Msg):
        async with MessageProgressUpdater(
            msg, nats_consumer_settings.nats_ack_wait * 0.66
        ):
            subject = msg.subject
            reply = msg.reply
            seqid = int(msg.reply.split(".")[5])

            split, work = self.processor.splitter(msg)
            logger.info(
                f"Message demultiplexed to split '{split}'",
                extra={"subject": subject, "reply": reply, "seqid": seqid},
            )

            worker, created = self.get_or_create_worker(split)

            seqid = int(msg.reply.split(".")[5])
            work_id = f"{split}:{seqid}"
            work_unit = WorkUnit(work_id, work)

            if work_id in self.working_on:
                logger.warning(
                    f"Trying to process again an already received NATS message. Skipping (workid={work_id})"
                )
                return

            await worker.add_work(work_unit)
            self.working_on.add(work_id)

            if created or (not created and not worker.working):
                # The second condition should never happen, the worker task finished
                # between us getting the worker and adding work. We'll create a new
                # task for it
                self.start_worker(split)

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
            work_unit_done.remove_listener(work_id)
            self.working_on.discard(work_id)
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
            worker = DemuxWorker(queue, self.processor.process)
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
            raise task.exception()  # type: ignore

    @staticmethod
    async def on_work_unit_done(
        work_id: str,
        notifier: asyncio.Future,
        payload: WorkOutcome,
    ):
        if payload.work_id == work_id:
            notifier.set_result(payload)


class DemuxWorker:
    def __init__(
        self,
        work_queue: asyncio.Queue,
        cb: Callable[[DemuxProcessorWork], Awaitable[bool]],
    ):
        self.cb = cb
        self.work_queue = work_queue
        self.working = False

    async def add_work(self, work: WorkUnit):
        await self.work_queue.put(work)

    async def work_until_finish(self):
        self.working = True
        while not self.work_queue.empty():
            print(
                "---- QUEUE {} -------------------------------------------------".format(
                    self.work_queue.qsize()
                )
            )
            from nucliadb_utils.settings import nats_consumer_settings

            print("Consumer settings:", nats_consumer_settings.nats_max_ack_pending)
            print(self.work_queue)
            print("----------------------------------------------------------------")
            work: WorkUnit = await self.work_queue.get()
            await self.do_work(work)
        self.working = False

    async def do_work(self, work: WorkUnit):
        try:
            logger.info(f"Working on '{work.work_id}'", extra={"work_id": work.work_id})
            success = await self.cb(work.work)
        except Exception as exc:
            event_id = capture_exception(exc)
            logger.error(
                "An error happened on nats demux worker. Check sentry for more details. "
                f"Event id: {event_id}",
                extra={"work_id": work.work_id},
                exc_info=exc,
            )
            await work_unit_done.dispatch(WorkOutcome(work_id=work.work_id, error=exc))
        else:
            await work_unit_done.dispatch(
                WorkOutcome(work_id=work.work_id, success=success)
            )
