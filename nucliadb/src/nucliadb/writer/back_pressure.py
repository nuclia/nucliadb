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
#

import asyncio
import contextlib
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from async_lru import alru_cache
from cachetools import TTLCache
from fastapi import HTTPException, Request

from nucliadb.common import datamanagers
from nucliadb.common.cluster.manager import get_index_nodes
from nucliadb.common.context import ApplicationContext
from nucliadb.common.context.fastapi import get_app_context
from nucliadb.common.http_clients.processing import ProcessingHTTPClient
from nucliadb.writer import logger
from nucliadb.writer.settings import back_pressure_settings as settings
from nucliadb_protos.writer_pb2 import ShardObject
from nucliadb_telemetry import metrics
from nucliadb_utils import const
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import is_onprem_nucliadb

__all__ = ["maybe_back_pressure"]


back_pressure_observer = metrics.Observer("nucliadb_back_pressure", labels={"type": ""})


RATE_LIMITED_REQUESTS_COUNTER = metrics.Counter(
    "nucliadb_rate_limited_requests", labels={"type": "", "cached": ""}
)


@dataclass
class BackPressureData:
    type: str
    try_after: datetime


class BackPressureException(Exception):
    def __init__(self, data: BackPressureData):
        self.data = data


def is_back_pressure_enabled() -> bool:
    return settings.enabled


class BackPressureCache:
    """
    Global cache for storing already computed try again in times.

    It allows us to avoid making the same calculations multiple
    times if back pressure has been applied.
    """

    def __init__(self):
        self._cache = TTLCache(maxsize=1024, ttl=5 * 60)
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[BackPressureData]:
        with self._lock:
            data = self._cache.get(key, None)
            if data is None:
                return None
            if datetime.utcnow() >= data.try_after:
                # The key has expired, so remove it from the cache
                self._cache.pop(key, None)
                return None
            return data

    def set(self, key: str, data: BackPressureData):
        with self._lock:
            self._cache[key] = data


_cache = BackPressureCache()


@contextlib.contextmanager
def cached_back_pressure(kbid: str, resource_uuid: Optional[str] = None):
    """
    Context manager that handles the caching of the try again in time so that
    we don't recompute try again times if we have already applied back pressure.
    """

    cache_key = "-".join([kbid, resource_uuid or ""])

    data: Optional[BackPressureData] = _cache.get(cache_key)
    if data is not None:
        try_after = data.try_after
        back_pressure_type = data.type
        RATE_LIMITED_REQUESTS_COUNTER.inc({"type": back_pressure_type, "cached": "true"})
        logger.info(
            "Back pressure applied from cache",
            extra={
                "type": back_pressure_type,
                "try_after": try_after,
                "kbid": kbid,
                "resource_uuid": resource_uuid,
            },
        )
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {try_after}",
                "try_after": try_after.timestamp(),
                "back_pressure_type": back_pressure_type,
            },
        )
    try:
        yield
    except BackPressureException as exc:
        try_after = exc.data.try_after
        back_pressure_type = exc.data.type
        RATE_LIMITED_REQUESTS_COUNTER.inc({"type": back_pressure_type, "cached": "false"})
        _cache.set(cache_key, exc.data)
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {try_after}",
                "try_after": try_after.timestamp(),
                "back_pressure_type": back_pressure_type,
            },
        )


class Materializer:
    """
    Singleton class that will run in the background gathering the different
    stats to apply back pressure and materializing it in memory. This allows us
    to do stale-reads when checking if back pressure is needed for a particular
    request - thus not slowing it down.
    """

    def __init__(
        self,
        nats_manager: NatsConnectionManager,
        indexing_check_interval: int = 30,
        ingest_check_interval: int = 30,
    ):
        self.nats_manager = nats_manager
        self.processing_http_client = ProcessingHTTPClient()

        self.indexing_check_interval = indexing_check_interval
        self.ingest_check_interval = ingest_check_interval

        self.ingest_pending: int = 0
        self.indexing_pending: dict[str, int] = {}

        self._tasks: list[asyncio.Task] = []
        self._running = False

        self.processing_pending_cache = TTLCache(maxsize=1024, ttl=60)  # type: ignore
        self.processing_pending_locks: dict[str, asyncio.Lock] = {}

    async def start(self):
        self._tasks.append(asyncio.create_task(self._get_indexing_pending_task()))
        self._tasks.append(asyncio.create_task(self._get_ingest_pending_task()))
        self._running = True

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        await self.processing_http_client.close()
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    async def get_processing_pending(self, kbid: str) -> int:
        """
        We don't materialize the pending messages for every kbid, but values are cached for some time.
        """
        cached = self.processing_pending_cache.get(kbid)
        if cached is not None:
            return cached

        lock = self.processing_pending_locks.setdefault(kbid, asyncio.Lock())
        async with lock:
            # Check again if the value has been cached while we were waiting for the lock
            cached = self.processing_pending_cache.get(kbid)
            if cached is not None:
                return cached

            # Get the pending messages and cache the result
            try:
                with back_pressure_observer({"type": "get_processing_pending"}):
                    pending = await self._get_processing_pending(kbid)
            except Exception:
                # Do not cache if there was an error
                logger.exception(
                    "Error getting pending messages to process. Back pressure on proccessing for KB can't be applied.",
                    exc_info=True,
                    extra={"kbid": kbid},
                )
                return 0

            if pending > 0:
                logger.info(
                    f"Processing returned {pending} pending messages for KB",
                    extra={"kbid": kbid},
                )
            self.processing_pending_cache[kbid] = pending
            return pending

    async def _get_processing_pending(self, kbid: str) -> int:
        response = await self.processing_http_client.stats(kbid=kbid, timeout=0.5)
        return response.incomplete

    def get_indexing_pending(self) -> dict[str, int]:
        return self.indexing_pending

    def get_ingest_pending(self) -> int:
        return self.ingest_pending

    async def _get_indexing_pending_task(self):
        try:
            while True:
                for node in get_index_nodes():
                    try:
                        with back_pressure_observer({"type": "get_indexing_pending"}):
                            self.indexing_pending[node.id] = await get_nats_consumer_pending_messages(
                                self.nats_manager,
                                stream=const.Streams.INDEX.name,
                                consumer=const.Streams.INDEX.group.format(node=node.id),
                            )
                    except Exception:
                        logger.exception(
                            "Error getting pending messages to index",
                            exc_info=True,
                            extra={"node_id": node.id},
                        )
                await asyncio.sleep(self.indexing_check_interval)
        except asyncio.CancelledError:
            pass

    async def _get_ingest_pending_task(self):
        try:
            while True:
                try:
                    with back_pressure_observer({"type": "get_ingest_pending"}):
                        self.ingest_pending = await get_nats_consumer_pending_messages(
                            self.nats_manager,
                            stream=const.Streams.INGEST_PROCESSED.name,
                            consumer=const.Streams.INGEST_PROCESSED.group,
                        )
                except Exception:
                    logger.exception(
                        "Error getting pending messages to ingest",
                        exc_info=True,
                    )
                await asyncio.sleep(self.ingest_check_interval)
        except asyncio.CancelledError:
            pass


MATERIALIZER: Optional[Materializer] = None
materializer_lock = threading.Lock()


async def start_materializer(context: ApplicationContext):
    global MATERIALIZER
    if MATERIALIZER is not None:
        logger.info("Materializer already started")
        return
    with materializer_lock:
        if MATERIALIZER is not None:
            return
        logger.info("Initializing materializer")
        try:
            nats_manager = context.nats_manager
        except AttributeError:
            logger.warning(
                "Could not initialize materializer. Nats manager not found or not initialized yet"
            )
            return
        materializer = Materializer(
            nats_manager,
            indexing_check_interval=settings.indexing_check_interval,
            ingest_check_interval=settings.ingest_check_interval,
        )
        await materializer.start()
        MATERIALIZER = materializer


async def stop_materializer():
    global MATERIALIZER
    if MATERIALIZER is None or not MATERIALIZER.running:
        logger.info("Materializer already stopped")
        return
    with materializer_lock:
        if MATERIALIZER is None:
            return
        logger.info("Stopping materializer")
        await MATERIALIZER.stop()
        MATERIALIZER = None


def get_materializer() -> Materializer:
    global MATERIALIZER
    if MATERIALIZER is None:
        raise RuntimeError("Materializer not initialized")
    return MATERIALIZER


async def maybe_back_pressure(request: Request, kbid: str, resource_uuid: Optional[str] = None) -> None:
    """
    This function does system checks to see if we need to put back pressure on writes.
    In that case, a HTTP 429 will be raised with the estimated time to try again.
    """
    if not is_back_pressure_enabled() or is_onprem_nucliadb():
        return
    await back_pressure_checks(request, kbid, resource_uuid)


async def back_pressure_checks(request: Request, kbid: str, resource_uuid: Optional[str] = None):
    """
    Will raise a 429 if back pressure is needed:
    - If the processing engine is behind.
    - If ingest processed consumer is behind.
    - If the indexing on nodes affected by the request (kbid, and resource_uuid) is behind.
    """
    context = get_app_context(request.app)
    materializer = get_materializer()
    with cached_back_pressure(kbid, resource_uuid):
        check_ingest_behind(materializer.get_ingest_pending())
        await check_indexing_behind(context, kbid, resource_uuid, materializer.get_indexing_pending())
        await check_processing_behind(materializer, kbid)


async def check_processing_behind(materializer: Materializer, kbid: str):
    """
    This function checks if the processing engine is behind and may raise a 429
    if it is further behind than the configured threshold.
    """
    max_pending = settings.max_processing_pending
    if max_pending <= 0:
        # Processing back pressure is disabled
        return

    kb_pending = await materializer.get_processing_pending(kbid)
    if kb_pending > max_pending:
        try_after = estimate_try_after(
            rate=settings.processing_rate,
            pending=kb_pending,
            max_wait=settings.max_wait_time,
        )
        data = BackPressureData(type="processing", try_after=try_after)
        logger.info(
            "Processing back pressure applied",
            extra={
                "kbid": kbid,
                "try_after": try_after,
                "pending": kb_pending,
            },
        )
        raise BackPressureException(data)


async def check_indexing_behind(
    context: ApplicationContext,
    kbid: str,
    resource_uuid: Optional[str],
    pending_by_node: dict[str, int],
):
    """
    If a resource uuid is provided, it will check the nodes that have the replicas
    of the resource's shard, otherwise it will check the nodes of all active shards
    for the KnowledgeBox.
    """
    max_pending = settings.max_indexing_pending
    if max_pending <= 0:
        # Indexing back pressure is disabled
        return

    if len(pending_by_node) == 0:
        logger.warning("No nodes found to check for pending messages")
        return

    # Get nodes that are involved in the indexing of the request
    if resource_uuid is not None:
        nodes_to_check = await get_nodes_for_resource_shard(context, kbid, resource_uuid)
    else:
        nodes_to_check = await get_nodes_for_kb_active_shards(context, kbid)

    if len(nodes_to_check) == 0:
        logger.warning(
            "No nodes found to check for pending messages",
            extra={"kbid": kbid, "resource_uuid": resource_uuid},
        )
        return

    # Get the highest pending value
    highest_pending = 0
    for node in nodes_to_check:
        if node not in pending_by_node:
            logger.warning("Node not found in pending messages", extra={"node": node})
            continue
        if pending_by_node[node] > highest_pending:
            highest_pending = pending_by_node[node]

    if highest_pending > max_pending:
        try_after = estimate_try_after(
            rate=settings.indexing_rate,
            pending=highest_pending,
            max_wait=settings.max_wait_time,
        )
        data = BackPressureData(type="indexing", try_after=try_after)
        logger.info(
            "Indexing back pressure applied",
            extra={
                "kbid": kbid,
                "resource_uuid": resource_uuid,
                "try_after": try_after,
                "pending": highest_pending,
            },
        )
        raise BackPressureException(data)


def check_ingest_behind(ingest_pending: int):
    max_pending = settings.max_ingest_pending
    if max_pending <= 0:
        # Ingest back pressure is disabled
        return

    if ingest_pending > max_pending:
        try_after = estimate_try_after(
            rate=settings.ingest_rate,
            pending=ingest_pending,
            max_wait=settings.max_wait_time,
        )
        data = BackPressureData(type="ingest", try_after=try_after)
        logger.info(
            "Ingest back pressure applied",
            extra={"try_after": try_after, "pending": ingest_pending},
        )
        raise BackPressureException(data)


def estimate_try_after(rate: float, pending: int, max_wait: int) -> datetime:
    """
    This function estimates the time to try again based on the rate and the number of pending messages.
    """
    delta_seconds = min(pending / rate, max_wait)
    return datetime.utcnow() + timedelta(seconds=delta_seconds)


@alru_cache(maxsize=1024, ttl=60 * 15)
async def get_nodes_for_kb_active_shards(context: ApplicationContext, kbid: str) -> list[str]:
    with back_pressure_observer({"type": "get_kb_active_shard"}):
        active_shard = await get_kb_active_shard(context, kbid)
    if active_shard is None:
        # KB doesn't exist or has been deleted
        logger.debug("No active shard found for KB", extra={"kbid": kbid})
        return []
    return [replica.node for replica in active_shard.replicas]


@alru_cache(maxsize=1024, ttl=60 * 60)
async def get_nodes_for_resource_shard(
    context: ApplicationContext, kbid: str, resource_uuid: str
) -> list[str]:
    with back_pressure_observer({"type": "get_resource_shard"}):
        resource_shard = await get_resource_shard(context, kbid, resource_uuid)
    if resource_shard is None:
        # Resource doesn't exist or KB has been deleted
        return []
    return [replica.node for replica in resource_shard.replicas]


async def get_nats_consumer_pending_messages(
    nats_manager: NatsConnectionManager, *, stream: str, consumer: str
) -> int:
    # get raw js client
    js = nats_manager.js
    consumer_info = await js.consumer_info(stream, consumer)
    return consumer_info.num_pending


async def get_kb_active_shard(context: ApplicationContext, kbid: str) -> Optional[ShardObject]:
    async with context.kv_driver.transaction(read_only=True) as txn:
        return await context.shard_manager.get_current_active_shard(txn, kbid)


async def get_resource_shard(
    context: ApplicationContext, kbid: str, resource_uuid: str
) -> Optional[ShardObject]:
    async with datamanagers.with_ro_transaction() as txn:
        shard_id = await datamanagers.resources.get_resource_shard_id(txn, kbid=kbid, rid=resource_uuid)
        if shard_id is None:
            # Resource does not exist
            logger.debug(
                "Resource shard not found",
                extra={"kbid": kbid, "resource_uuid": resource_uuid},
            )
            return None

        all_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if all_shards is None:
            # KB doesn't exist or has been deleted
            logger.debug("No shards found for KB", extra={"kbid": kbid})
            return None

    for shard in all_shards.shards:
        if shard.shard == shard_id:
            return shard
    else:
        logger.error(
            "Resource shard not found",
            extra={"kbid": kbid, "resource_uuid": resource_uuid, "shard_id": shard_id},
        )
        return None
