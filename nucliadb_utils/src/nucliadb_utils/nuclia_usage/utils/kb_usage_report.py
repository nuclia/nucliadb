# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging
from collections.abc import Iterable
from contextlib import suppress
from datetime import datetime, timezone

from nats.js.client import JetStreamContext

from nucliadb_protos.kb_usage_pb2 import (
    ActivityLogMatch,
    KBSource,
    KbUsage,
    Predict,
    Process,
    Search,
    Service,
    Storage,
)
from nucliadb_telemetry.jetstream import JetStreamContextTelemetry

logger = logging.getLogger(__name__)


class KbUsageReportUtility:
    queue: asyncio.Queue

    def __init__(
        self,
        nats_stream: JetStreamContext | JetStreamContextTelemetry,
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
            except Exception:
                logger.exception("Could not send KbUsage message")
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
        service: Service.ValueType,
        account_id: str | None,
        kb_id: str | None,
        kb_source: KBSource.ValueType,
        processes: Iterable[Process] = (),
        predicts: Iterable[Predict] = (),
        searches: Iterable[Search] = (),
        storage: Storage | None = None,
        activity_log_match: ActivityLogMatch | None = None,
    ):
        usage = KbUsage()
        usage.service = service
        usage.timestamp.FromDatetime(datetime.now(tz=timezone.utc))
        if account_id is not None:
            usage.account_id = account_id
        if kb_id is not None:
            usage.kb_id = kb_id
        usage.kb_source = kb_source
        if activity_log_match is not None:
            if activity_log_match.id:
                usage.activity_log_match.id = activity_log_match.id
                usage.activity_log_match.type = activity_log_match.type

        usage.processes.extend(processes)
        usage.predicts.extend(predicts)
        usage.searches.extend(searches)
        if storage is not None:
            for field, value in storage.ListFields():
                setattr(usage.storage, field.name, value)

        self.send(usage)
