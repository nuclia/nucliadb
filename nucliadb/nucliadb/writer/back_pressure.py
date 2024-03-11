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
import threading
from datetime import datetime, timedelta
from typing import Optional

import nats
from async_lru import alru_cache
from cachetools import TTLCache
from fastapi import HTTPException, Request
from nucliadb_protos.writer_pb2 import ShardObject

from nucliadb.common.context import ApplicationContext
from nucliadb.common.context.fastapi import get_app_context
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.common.http_clients.processing import ProcessingHTTPClient
from nucliadb.writer import logger
from nucliadb.writer.settings import back_pressure_settings as settings
from nucliadb_telemetry import errors
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


def is_back_pressure_enabled() -> bool:
    return settings.enabled


async def maybe_back_pressure(
    request: Request, kbid: str, resource_uuid: Optional[str] = None
) -> None:
    """
    This function does system checks to see if we need to put back pressure on writes.
    Back pressure is applied when the system is processing behind or when the nodes are indexing behind.
    In that case, a HTTP 429 will be raised with the estimated time to try again.
    """
    if not is_back_pressure_enabled():
        return

    if is_onprem_nucliadb():
        return

    context = get_app_context(request.app)
    tasks = [
        asyncio.create_task(check_processing_behind()),
        asyncio.create_task(check_ingest_behind(context)),
        asyncio.create_task(check_indexing_behind(context, kbid, resource_uuid)),
    ]
    await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)


async def check_processing_behind():
    """
    This function checks if the processing engine is behind and may raise a 429
    if it is further behind than the configured threshold.
    """
    max_pending = settings.max_processing_pending
    if max_pending <= 0:
        # Processing back pressure is disabled
        return

    # If we have already applied back pressure, we don't need to check
    # again until the try again in time has expired
    try_after = try_after_cache.get(key="processing")
    if try_after is not None:
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {try_after}",
                "try_after": try_after.timestamp(),
                "back_pressure_type": "processing",
            },
        )

    pending = await get_pending_to_process()
    if pending > max_pending:
        try_after = estimate_try_after_from_rate(
            rate=settings.processing_rate, pending=pending
        )
        try_after_cache.set("processing", try_after)
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {try_after}",
                "try_after": try_after.timestamp(),
                "back_pressure_type": "processing",
            },
        )


async def check_indexing_behind(
    context: ApplicationContext, kbid: str, resource_uuid: Optional[str] = None
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

    # If we have already applied back pressure, we don't need to check again
    # until the try again in time has expired
    try_after = try_after_cache.get(f"{kbid}::{resource_uuid}")
    if try_after is not None:
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {try_after}",
                "try_after": try_after.timestamp(),
                "back_pressure_type": "indexing",
            },
        )

    pending_by_node = await get_pending_to_index(
        context, kbid, resource_uuid=resource_uuid
    )
    if len(pending_by_node) == 0:
        return

    highest_pending = max(pending_by_node.items(), key=lambda x: x[1])[1]
    if highest_pending > max_pending:
        try_after = estimate_try_after_from_rate(
            rate=settings.indexing_rate, pending=highest_pending
        )
        try_after_cache.set(f"{kbid}::{resource_uuid}", try_after)
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {try_after}",
                "try_after": try_after.timestamp(),
                "back_pressure_type": "indexing",
            },
        )


async def check_ingest_behind(context: ApplicationContext):
    """
    Checks if the ingest processed consumer is behind and may raise a 429
    """
    max_pending = settings.max_ingest_pending
    if max_pending <= 0:
        # Indexing back pressure is disabled
        return

    # If we have already applied back pressure, we don't need to check again
    # until the try again in time has expired
    try_after = try_after_cache.get("ingest")
    if try_after is not None:
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {try_after}",
                "try_after": try_after.timestamp(),
                "back_pressure_type": "ingest",
            },
        )

    pending = await get_pending_to_ingest(context)
    if pending > max_pending:
        try_after = estimate_try_after_from_rate(
            rate=settings.ingest_rate, pending=pending
        )
        try_after_cache.set("ingest", try_after)
        raise HTTPException(
            status_code=429,
            detail={
                "message": f"Too many messages pending to ingest. Retry after {try_after}",
                "try_after": try_after.timestamp(),
                "back_pressure_type": "ingest",
            },
        )


def estimate_try_after_from_rate(rate: float, pending: int) -> datetime:
    """
    This function estimates the time to try again based on the rate and the number of pending messages.
    """
    delta_seconds = pending / rate
    return datetime.utcnow() + timedelta(seconds=delta_seconds)


@alru_cache(maxsize=1024, ttl=60)
async def get_pending_to_process() -> int:
    async with ProcessingHTTPClient() as processing_http_client:
        try:
            response = await processing_http_client.stats()
            return response.incomplete + response.scheduled
        except Exception:
            logger.exception(
                "Error getting pending messages to process. Global processing back pressure will not be applied.",  # noqa
                exc_info=True,
            )
            return 0


async def get_pending_to_index(
    context: ApplicationContext, kbid: str, resource_uuid: Optional[str] = None
) -> dict[str, int]:
    """
    This function gets the number of pending messages to index for all involved nodes.
    """
    results: dict[str, int] = {}

    # Schedule the tasks
    if resource_uuid is not None:
        nodes_to_check = await get_nodes_for_resource_shard(
            context, kbid, resource_uuid
        )
    else:
        nodes_to_check = await get_nodes_for_kb_active_shards(context, kbid)

    tasks = []
    for node in nodes_to_check:
        tasks.append(asyncio.create_task(get_node_pending_messages(context, node)))

    if not tasks:
        return results

    # Wait for all tasks to finish
    done_tasks, pending_tasks = await asyncio.wait(tasks)

    # Process the results
    assert len(pending_tasks) == 0
    for done_task in done_tasks:
        exception = done_task.exception()
        if exception is not None:
            errors.capture_exception(exception)
            logger.error(
                f"Error getting pending messages to index: {exception}",
                extra={"kbid": kbid},
            )
            continue
        node, pending_messages = done_task.result()
        results[node] = pending_messages

    return results


@alru_cache(maxsize=None, ttl=30)
async def get_pending_to_ingest(context: ApplicationContext) -> int:
    try:
        return await get_nats_consumer_pending_messages(
            context,
            stream=const.Streams.INGEST_PROCESSED.name,
            consumer=const.Streams.INGEST_PROCESSED.subject,
        )
    except nats.js.errors.NotFoundError:
        logger.warning(
            "Ingest processed consumer not found",
            extra={"stream": const.Streams.INGEST_PROCESSED.name},
        )
        return 0


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


@alru_cache(maxsize=None, ttl=30)
async def get_node_pending_messages(
    context: ApplicationContext, node_id: str
) -> tuple[str, int]:
    try:
        num_pending = await get_nats_consumer_pending_messages(
            context,
            stream=const.Streams.INDEX.name,
            consumer=const.Streams.INDEX.group.format(node=node_id),
        )
        return node_id, num_pending
    except nats.js.errors.NotFoundError:
        # These handles the case for when the node is added to
        # the cluster but it doesn't have a consumer yet.
        logger.warning(
            "Consumer not found",
            extra={"stream": const.Streams.INDEX.name, "node_id": node_id},
        )
        return node_id, 0


async def get_nats_consumer_pending_messages(
    context: ApplicationContext, *, stream: str, consumer: str
) -> int:
    nats_manager: NatsConnectionManager = context.nats_manager
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
