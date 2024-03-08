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
from dataclasses import dataclass
from re import A
from typing import Optional

import nats
from fastapi import HTTPException

from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.writer import logger
from nucliadb.writer.settings import rate_limit_settings as settings
from nucliadb_telemetry import errors
from nucliadb_utils import const
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import is_onprem_nucliadb


@dataclass
class NodeIndexingStatus:
    node_id: str
    pending: int


async def maybe_rate_limit_kb_writes(
    context: ApplicationContext, kbid: str, resource_uuid: Optional[str] = None
):
    """
    This function does system checks to see if writes need to be rate limited.
    If a resource uuid is provided, it will check the nodes of the resource shard, otherwise it
    will check the nodes of the KB active shard.
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
    tasks = []
    async for node in iter_nodes(context, kbid, resource_uuid=resource_uuid):
        tasks.append(asyncio.create_task(check_node_indexing_behind(context, node)))
    if not tasks:
        return

    results: list[NodeIndexingStatus] = []
    try:
        done, pending = await asyncio.wait(*tasks)
        assert len(pending) == 0
        done_task: asyncio.Task
        for done_task in done:
            if done_task.exception() is not None:
                raise done_task.exception()
            result = done_task.result()
            if result is not None:
                results.append(result)
    except Exception as exc:
        errors.capture_exception(exc)
        logger.error(
            "Unexpected errors trying to rate limit. Rate limits will not be applied.",
            extra={"error": str(exc)},
        )
        return

    if len(results) == 0:
        return
    max_behind = max(result.pending for result in results)
    if max_behind > settings.max_node_indexing_pending:
        raise HTTPException(
            status_code=429,
            detail={
                "message": "Too many pending to index",
                "pending": max_behind,
                "try_again_in": estimate_try_again_in(max_behind),
            },
        )


def estimate_try_again_in(pending: int) -> float:
    """
    This function estimates the time in seconds to try again based on the pending messages to index
    """
    return pending / settings.estimation_indexing_rate


async def iter_nodes(
    context: ApplicationContext, kbid: str, resource_uuid: Optional[str] = None
) -> AsyncGenerator[str, None, None]:
    if resource_uuid is not None:
        async for node in iter_nodes_for_resource_shard(context, kbid, resource_uuid):
            yield node
    else:
        async for node in iter_nodes_for_kb_active_shards(context, kbid):
            yield node


async def iter_nodes_for_kb_active_shards(
    context: ApplicationContext, kbid: str
) -> AsyncGenerator[str, None, None]:
    async with context.kv_driver.transaction() as txn:
        active_shard = await context.shard_manager.get_current_active_shard(txn, kbid)
        if active_shard is None:
            logger.warning("No active shard found for KB", extra={"kbid": kbid})
            return
    for replica in active_shard.replicas:
        yield replica.node


async def iter_nodes_for_resource_shard(
    context: ApplicationContext, kbid: str, resource_uuid: str
) -> AsyncGenerator[str, None, None]:
    rdm = ResourcesDataManager(driver=context.kv_driver, storage=context.blob_storage)
    shard_id = await rdm.get_resource_shard_id(kbid, resource_uuid)
    all_shards = await context.shard_manager.get_all_shards(kbid)
    shard_found = False
    for shard in all_shards:
        if shard.uuid == shard_id:
            shard_found = True
            for replica in shard.replicas:
                yield replica.node
    if not shard_found:
        logger.warning(
            "Resource shard not found",
            extra={"kbid": kbid, "resource_uuid": resource_uuid},
        )


async def check_node_indexing_behind(context: ApplicationContext, node_id: str) -> bool:
    pending = await get_pending_to_index(context, node_id)
    if pending is None:
        return None
    return NodeIndexingStatus(node_id=node_id, pending=pending)


async def get_pending_to_index(
    context: ApplicationContext, node_id: str
) -> Optional[int]:
    try:
        nats_manager: NatsConnectionManager = context.nats_manager
        # get raw js client
        js = getattr(nats_manager.js, "js", nats_manager.js)
        try:
            consumer_info = await js.consumer_info(
                const.Streams.INDEX.name,
                const.Streams.INDEX.group.format(node=node_id),
            )
            return consumer_info.num_pending
        except nats.js.errors.NotFoundError:
            logger.warning(
                "Consumer not found",
                extra={"stream": const.Streams.INDEX.name, "node_id": node_id},
            )
            return 0
    except Exception as exc:
        errors.capture_exception(exc)
        logger.error(
            "Error getting pending to index",
            extra={"node": node_id, "error": str(exc)},
        )
        return None
