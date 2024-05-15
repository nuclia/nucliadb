import asyncio
import logging
from contextlib import suppress
from datetime import datetime, timezone
from typing import Optional, Union

from nats.js.client import JetStreamContext
from sentry_sdk import capture_exception

from nucliadb_utils.nuclia_usage.protos.kb_usage_pb2 import (
    KBSource,
    KbUsage,
    Predict,
    Process,
    Search,
    Service,
    Storage,
)

logger = logging.getLogger(__name__)


class KbUsageReportUtility:
    queue: asyncio.Queue
    lock: asyncio.Lock

    def __init__(
        self,
        nats_stream: JetStreamContext,
        nats_subject: str,
        max_queue_size: int = 100,
    ):
        self.nats_stream = nats_stream
        self.nats_subject = nats_subject
        self.queue = asyncio.Queue(max_queue_size)
        self.task = None

    async def initialize(self):
        if self.task is None:
            self.task = asyncio.create_task(self.run())

    async def finalize(self):
        if self.task is not None:
            self.task.cancel()
            with suppress(asyncio.CancelledError, asyncio.exceptions.TimeoutError):
                await asyncio.wait_for(self.task, timeout=2)

    async def run(self) -> None:
        while True:
            message: KbUsage = await self.queue.get()
            try:
                await self._send(message)
            except Exception as e:
                logger.exception("Could not send KbUsage message")
                capture_exception(e)
            finally:
                self.queue.task_done()

    def send(self, message: KbUsage):
        try:
            self.queue.put_nowait(message)
        except asyncio.QueueFull:
            logger.warning("KbUsage utility queue is full, dropping message")

    async def _send(self, message: KbUsage) -> int:
        res = await self.nats_stream.publish(
            self.nats_subject,
            message.SerializeToString(),
        )
        return res.seq

    def send_kb_usage(
        self,
        service: Service,
        account_id: Optional[str],
        kb_id: Optional[str],
        kb_source: KBSource,
        processes: list[Process] = [],
        predicts: list[Predict] = [],
        searches: list[Search] = [],
        storage: Optional[Storage] = None,
    ):
        usage = KbUsage()
        usage.service = service
        usage.timestamp.FromDatetime(datetime.now(tz=timezone.utc))
        if account_id is not None:
            usage.account_id = account_id
        if kb_id is not None:
            usage.kb_id = kb_id
        usage.kb_source = kb_source

        usage.processes.extend(processes)
        usage.predicts.extend(predicts)
        usage.searches.extend(searches)
        if storage is not None:
            if storage.HasField("fields"):
                usage.storage.fields = storage.fields
            if storage.HasField("paragraphs"):
                usage.storage.paragraphs = storage.paragraphs
            if storage.HasField("resources"):
                usage.storage.resources = storage.resources

        self.send(usage)


class DummyKbUsageReportUtility(KbUsageReportUtility):
    queue: asyncio.Queue

    def __init__(self):
        self.queue = asyncio.Queue()

    async def initialize(self):
        pass

    async def finalize(self):
        pass


KbUsageReportUtilType = Union[KbUsageReportUtility, DummyKbUsageReportUtility, None]
