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
from collections.abc import AsyncGenerator
from typing import Optional

import nats
from async_lru import alru_cache
from fastapi import HTTPException, Request
from nucliadb_protos.writer_pb2 import ShardObject

from nucliadb.common.context import ApplicationContext
from nucliadb.common.context.fastapi import get_app_context
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.writer import logger
from nucliadb.writer.settings import back_pressure_settings as settings
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import is_onprem_nucliadb

"""
TODO:
- Add caching
- Add tests
"""


async def maybe_back_pressure(
    request: Request, kbid: str, resource_uuid: Optional[str] = None
):
    context = get_app_context(request.app)
    await maybe_back_pressure_kb_writes(context, kbid, resource_uuid=resource_uuid)


async def maybe_back_pressure_kb_writes(
    context: ApplicationContext, kbid: str, resource_uuid: Optional[str] = None
):
    """
    This function does system checks to see if we need to put back pressure on writes.
    Back pressure is applied when the system is processing behind or when the nodes are indexing behind.
    In that case, a HTTP 429 will be raised with the estimated time to try again.

    """
    await check_processing_behind(context, kbid)
    if not is_onprem_nucliadb():
        await check_nodes_indexing_behind(context, kbid, resource_uuid)


async def check_processing_behind(context: ApplicationContext, kbid: str):
    # TODO
    pass


async def check_nodes_indexing_behind(
    context: ApplicationContext, kbid: str, resource_uuid: Optional[str] = None
):
    """
    If a resource uuid is provided, it will check the nodes that have the replicas
    of the resource's shard, otherwise it will check the nodes of all active shards
    for the KnowledgeBox.
    """

    pending_by_node = await get_pending_to_index(
        context, kbid, resource_uuid=resource_uuid
    )
    if len(pending_by_node) == 0:
        return

    max_pending = max(pending_by_node.items(), key=lambda x: x[1])[1]
    if max_pending > settings.max_node_indexing_pending:
        raise HTTPException(
            status_code=429,
            detail={
                "message": "Too many pending to index",
                "pending": max_pending,
                "try_again_in": estimate_try_again_in(max_pending),
            },
        )


async def get_pending_to_index(
    context: ApplicationContext, kbid: str, resource_uuid: Optional[str] = None
) -> dict[str, int]:
    """
    This function asynchronously gets the number of pending messages to index for all involved nodes.
    """
    results: dict[str, int] = {}
    tasks = []

    # Schedule the tasks
    async for node in iter_nodes_to_check(context, kbid, resource_uuid=resource_uuid):
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


def estimate_try_again_in(pending: int) -> float:
    """
    This function estimates the time in seconds to try again based on the pending messages to index
    """
    return pending / settings.estimation_indexing_rate


async def iter_nodes_to_check(
    context: ApplicationContext, kbid: str, resource_uuid: Optional[str] = None
) -> AsyncGenerator[str, None]:
    if resource_uuid is not None:
        async for node in iter_nodes_for_resource_shard(context, kbid, resource_uuid):
            yield node
    else:
        async for node in iter_nodes_for_kb_active_shards(context, kbid):
            yield node


async def iter_nodes_for_kb_active_shards(
    context: ApplicationContext, kbid: str
) -> AsyncGenerator[str, None]:
    active_shard = await get_kb_active_shard(context, kbid)
    if active_shard is None:
        # KB doesn't exist or has been deleted
        logger.debug("No active shard found for KB", extra={"kbid": kbid})
        return

    for replica in active_shard.replicas:
        yield replica.node


async def iter_nodes_for_resource_shard(
    context: ApplicationContext, kbid: str, resource_uuid: str
) -> AsyncGenerator[str, None]:
    resource_shard = await get_resource_shard(context, kbid, resource_uuid)
    if resource_shard is None:
        # Resource doesn't exist or KB has been deleted
        return

    for replica in resource_shard.replicas:
        yield replica.node


@alru_cache(maxsize=None, ttl=10)
async def get_node_pending_messages(
    context: ApplicationContext, node_id: str
) -> tuple[str, int]:
    nats_manager: NatsConnectionManager = context.nats_manager
    # get raw js client
    js = getattr(nats_manager.js, "js", nats_manager.js)
    try:
        consumer_info = await js.consumer_info(
            const.Streams.INDEX.name,
            const.Streams.INDEX.group.format(node=node_id),
        )
        return node_id, consumer_info.num_pending
    except nats.js.errors.NotFoundError:
        # These handles the case for when the node is added to
        # the cluster but it doesn't have a consumer yet.
        logger.warning(
            "Consumer not found",
            extra={"stream": const.Streams.INDEX.name, "node_id": node_id},
        )
        return node_id, 0


@alru_cache(maxsize=128, ttl=60 * 15)
async def get_kb_active_shard(
    context: ApplicationContext, kbid: str
) -> Optional[ShardObject]:
    async with context.kv_driver.transaction() as txn:
        return await context.shard_manager.get_current_active_shard(txn, kbid)


@alru_cache(maxsize=1024, ttl=60 * 60)
async def get_resource_shard(
    context: ApplicationContext, kbid: str, resource_uuid: str
):
    rdm = ResourcesDataManager(driver=context.kv_driver, storage=context.blob_storage)
    shard_id = await rdm.get_resource_shard_id(kbid, resource_uuid)
    if shard_id is None:
        # Resource does not exist
        logger.debug(
            "Resource shard not found",
            extra={"kbid": kbid, "resource_uuid": resource_uuid},
        )
        return

    async with context.kv_driver.transaction() as txn:
        all_shards = await context.shard_manager.get_all_shards(txn, kbid)
        if all_shards is None:
            # KB doesn't exist or has been deleted
            logger.debug("No shards found for KB", extra={"kbid": kbid})
            return

    found_shard = False
    for shard in all_shards.shards:
        if shard.shard == shard_id:
            found_shard = True
            return shard

    if not found_shard:
        logger.error(
            "Resource shard not found",
            extra={"kbid": kbid, "resource_uuid": resource_uuid, "shard_id": shard_id},
        )
        return
