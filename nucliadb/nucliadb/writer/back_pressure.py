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
from datetime import datetime, timedelta
from typing import Optional

from async_lru import alru_cache
from cachetools import TTLCache
from fastapi import HTTPException, Request
from nucliadb_protos.writer_pb2 import ShardObject

from nucliadb.common.cluster.manager import get_index_nodes
from nucliadb.common.context import ApplicationContext
from nucliadb.common.context.fastapi import get_app_context
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.common.http_clients.processing import ProcessingHTTPClient
from nucliadb.writer import logger
from nucliadb.writer.settings import back_pressure_settings as settings
from nucliadb_utils import const
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import is_onprem_nucliadb

"""
TODO:
- In the event of an unexpected error (tikv, nats, etc),
  should we fail hard or should we allow the write to go through?
- Make sure I didn't miss any endpoint to protect
- Add tests
- Add metrics / alerts
- Double check default values!
"""


__all__ = ["maybe_back_pressure"]


def is_back_pressure_enabled() -> bool:
    return settings.enabled


class TryAfterCache:
    """
    Global cache for storing already computed try again in times.

    It allows us to avoid making the same calculations multiple
    times if back pressure has been applied.
    """

    def __init__(self):
        self._cache = TTLCache(maxsize=1024, ttl=5 * 60)
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[datetime]:
        with self._lock:
            try_after = self._cache.get(key, None)
            if try_after is None:
                return None

            if datetime.utcnow() >= try_after:
                # The key has expired, so remove it from the cache
                self._cache.pop(key, None)
                return None

            return try_after

    def set(self, key: str, try_after: datetime):
        with self._lock:
            self._cache[key] = try_after


try_after_cache = TryAfterCache()


@contextlib.contextmanager
def cached_try_after(key: str, back_pressure_type: str):
    """
    Context manager that handles the caching of the try again in time so that
    we don't recompute try again times if we have already applied back pressure.
    """

    cached_value: Optional[datetime] = try_after_cache.get(key)
    if cached_value is not None:
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {cached_value}",
                "try_after": cached_value.timestamp(),
                "back_pressure_type": back_pressure_type,
            },
        )

    try:
        yield
    except HTTPException as http_exception:
        # If we get a 429, we store the try again in time in the cache
        if http_exception.status_code == 429:
            try_after = datetime.fromtimestamp(http_exception.detail["try_after"])
            try_after_cache.set(key, try_after)
        raise


class BackPressureSingleton:
    """
    Singleton class that will run in the background gatheringthe different stats to apply back pressure.
    This allows us to do back pressure checks and calculations at request time without blocking the request too much.
    """

    def __init__(self, nats_manager: NatsConnectionManager, check_interval: int = 30):
        self.nats_manager = nats_manager
        self.check_interval = check_interval
        self._tasks = []
        self.ingest_pending: int = 0
        self.indexing_pending: dict[str, int] = {}
        self.processing_http_client = ProcessingHTTPClient()

    async def start(self):
        self._tasks.append(asyncio.create_task(self._check_nodes_indexing_consumers()))
        self._tasks.append(asyncio.create_task(self._check_ingest_processed_consumer()))

    async def stop(self):
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        await self.processing_http_client.close()

    @alru_cache(maxsize=1024, ttl=60)
    async def get_kb_pending_to_process(self, kbid: str) -> int:
        response = await self.processing_http_client.stats(kbid=kbid)
        return response.incomplete + response.scheduled

    async def _check_nodes_indexing_consumers(self):
        try:
            while True:
                for node_id in get_index_nodes():
                    try:
                        self.indexing_pending[
                            node_id
                        ] = await get_nats_consumer_pending_messages(
                            self.nats_manager,
                            stream=const.Streams.INDEX.name,
                            consumer=const.Streams.INDEX.group.format(node=node_id),
                        )
                    except Exception:
                        logger.exception(
                            "Error getting pending messages to index",
                            exc_info=True,
                            extra={"node_id": node_id},
                        )
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            pass

    async def _check_ingest_processed_consumer(self):
        try:
            while True:
                try:
                    self.ingest_pending = await get_nats_consumer_pending_messages(
                        self.nats_manager,
                        stream=const.Streams.INGEST_PROCESSED.name,
                        consumer=const.Streams.INGEST_PROCESSED.subject,
                    )
                except Exception:
                    logger.exception(
                        "Error getting pending messages to ingest",
                        exc_info=True,
                    )
                await asyncio.sleep(self.check_interval)
        except asyncio.CancelledError:
            pass

    def get_values(self) -> dict:
        return {
            "ingest_pending": self.ingest_pending,
            "indexing_pending": self.indexing_pending,
        }


async def maybe_back_pressure(
    request: Request, kbid: str, resource_uuid: Optional[str] = None
) -> None:
    """
    This function does system checks to see if we need to put back pressure on writes.
    In that case, a HTTP 429 will be raised with the estimated time to try again.
    """
    if not is_back_pressure_enabled() or is_onprem_nucliadb():
        return
    await back_pressure_checks(request, kbid, resource_uuid)


async def back_pressure_checks(
    request: Request, kbid: str, resource_uuid: Optional[str] = None
):
    """
    Will raise a 429 if back pressure is needed:
    - If the processing engine is behind.
    - If the indexing on nodes affected by the request (kbid, and resource_uuid) is behind.
    """
    context = get_app_context(request.app)
    back_pressure = context.back_pressure_singleton
    if back_pressure is None:
        logger.error("Back pressure singleton not found")
        return

    await check_processing_behind(back_pressure, kbid)

    indexing_pending = back_pressure.get_values()["indexing_pending"]
    await check_indexing_behind(context, kbid, resource_uuid, indexing_pending)


async def check_processing_behind(back_pressure: BackPressureSingleton, kbid: str):
    """
    This function checks if the processing engine is behind and may raise a 429
    if it is further behind than the configured threshold.
    """
    max_pending = settings.max_processing_pending
    if max_pending <= 0:
        # Processing back pressure is disabled
        return

    with cached_try_after(f"processing::{kbid}", "processing"):
        kb_pending = await back_pressure.get_kb_pending_to_process(kbid)
        if kb_pending > max_pending:
            try_after = estimate_try_after(
                rate=settings.processing_rate, pending=kb_pending
            )
            raise HTTPException(
                status_code=429,
                detail={
                    "message": f"Too many messages pending to ingest. Retry after {try_after}",
                    "try_after": try_after.timestamp(),
                    "back_pressure_type": "processing",
                },
            )


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

    with cached_try_after(f"indexing::{kbid}::{resource_uuid}", "indexing"):
        if len(pending_by_node) == 0:
            logger.warning("No nodes found to check for pending messages")
            return

        # Get nodes that are involved in the indexing of the request
        if resource_uuid is not None:
            nodes_to_check = await get_nodes_for_resource_shard(
                context, kbid, resource_uuid
            )
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
                logger.warning(
                    "Node not found in pending messages", extra={"node": node}
                )
                continue
            if pending_by_node[node] > highest_pending:
                highest_pending = pending_by_node[node]

        if highest_pending > max_pending:
            try_after = estimate_try_after(
                rate=settings.indexing_rate, pending=highest_pending
            )
            raise HTTPException(
                status_code=429,
                detail={
                    "message": f"Too many messages pending to ingest. Retry after {try_after}",
                    "try_after": try_after.timestamp(),
                    "back_pressure_type": "indexing",
                },
            )


def estimate_try_after(rate: float, pending: int) -> datetime:
    """
    This function estimates the time to try again based on the rate and the number of pending messages.
    """
    delta_seconds = pending / rate
    return datetime.utcnow() + timedelta(seconds=delta_seconds)


@alru_cache(maxsize=1024, ttl=60 * 15)
async def get_nodes_for_kb_active_shards(
    context: ApplicationContext, kbid: str
) -> list[str]:
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
    resource_shard = await get_resource_shard(context, kbid, resource_uuid)
    if resource_shard is None:
        # Resource doesn't exist or KB has been deleted
        return []
    return [replica.node for replica in resource_shard.replicas]


async def get_nats_consumer_pending_messages(
    nats_manager: NatsConnectionManager, *, stream: str, consumer: str
) -> int:
    # get raw js client
    js = getattr(nats_manager.js, "js", nats_manager.js)
    consumer_info = await js.consumer_info(stream, consumer)
    return consumer_info.num_pending


async def get_kb_active_shard(
    context: ApplicationContext, kbid: str
) -> Optional[ShardObject]:
    async with context.kv_driver.transaction() as txn:
        return await context.shard_manager.get_current_active_shard(txn, kbid)


async def get_resource_shard(
    context: ApplicationContext, kbid: str, resource_uuid: str
) -> Optional[ShardObject]:
    rdm = ResourcesDataManager(driver=context.kv_driver, storage=context.blob_storage)
    shard_id = await rdm.get_resource_shard_id(kbid, resource_uuid)
    if shard_id is None:
        # Resource does not exist
        logger.debug(
            "Resource shard not found",
            extra={"kbid": kbid, "resource_uuid": resource_uuid},
        )
        return None

    async with context.kv_driver.transaction() as txn:
        all_shards = await context.shard_manager.get_all_shards(txn, kbid)
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
