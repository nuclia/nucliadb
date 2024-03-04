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
from functools import partial
from typing import Optional

from grpc import StatusCode
from grpc.aio import AioRpcError
from nucliadb_protos.nodewriter_pb2 import GarbageCollectorResponse

from nucliadb_node import logger, signals
from nucliadb_node.settings import settings
from nucliadb_node.signals import SuccessfulIndexingPayload
from nucliadb_node.writer import Writer
from nucliadb_telemetry import metrics
from nucliadb_telemetry.errors import capture_exception

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
                    # NOTE: garbage collector may not run if the shard is busy.
                    # We currently don't do anything to retry.
                    status = await self._writer.garbage_collector(self._shard_id)
                    if status.status == GarbageCollectorResponse.Status.OK:
                        logger.info(
                            "Garbage collection finished correctly",
                            extra={"shard": self._shard_id},
                        )
                    elif status.status == GarbageCollectorResponse.Status.TRY_LATER:
                        logger.warning(
                            "Garbage collection unavailable",
                            extra={"shard": self._shard_id},
                        )
            except AioRpcError as grpc_error:
                if grpc_error.code() == StatusCode.NOT_FOUND or (
                    grpc_error.code() == StatusCode.INTERNAL
                    and "Shard not found" in grpc_error.details()
                ):
                    logger.error(
                        "Shard does not exist and can't be garbage collected",
                        extra={"shard": self._shard_id},
                    )
                else:
                    event_id = capture_exception(grpc_error)
                    logger.exception(
                        f"Could not garbage collect. Check sentry for more details. Event id: {event_id}",
                        extra={"shard": self._shard_id},
                    )
            except Exception as exc:
                event_id = capture_exception(exc)
                logger.exception(
                    f"Could not garbage collect. Check sentry for more details. Event id: {event_id}",
                    extra={"shard": self._shard_id},
                )


class ShardGcScheduler:
    listener_id = "shard-gc-scheduler-{id}"

    def __init__(self, writer: Writer):
        self.writer = writer
        # right now, only allow one gc at a time but
        # can be expanded to allow multiple if we want with a semaphore
        self.gc_lock = asyncio.Semaphore(1)
        self.shard_managers: dict[str, ShardManager] = {}

    async def initialize(self):
        signals.successful_indexing.add_listener(
            self.listener_id.format(id=id(self)),
            partial(ShardGcScheduler.on_successful_indexing, self),
        )
        # Schedule garbage collection for all shards on startup
        await self.garbage_collect_all()

    async def finalize(self):
        signals.successful_indexing.remove_listener(
            self.listener_id.format(id=id(self))
        )

    def get_shard_manager(self, shard_id: str) -> ShardManager:
        if shard_id not in self.shard_managers:
            self.shard_managers[shard_id] = ShardManager(
                shard_id, self.writer, self.gc_lock
            )
        return self.shard_managers[shard_id]

    async def garbage_collect_all(self) -> None:
        for idx, shard in enumerate((await self.writer.shards()).ids):
            sm = self.get_shard_manager(shard.id)
            sm.shard_changed_event(idx * 0.01)

    async def on_successful_indexing(self, payload: SuccessfulIndexingPayload):
        sm = self.get_shard_manager(payload.index_message.shard)
        async with sm.lock:
            sm.shard_changed_event()
