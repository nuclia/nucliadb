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
import json
from typing import List, Optional

from fastapi import HTTPException, Request
from fastapi_versioning import version
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.noderesources_pb2 import Shard
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject
from nucliadb_protos.writer_pb2 import Shards

from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.manager import choose_node
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG_BASE
from nucliadb.ingest.txn_utils import abort_transaction
from nucliadb.search import logger
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.search.shards import get_shard
from nucliadb.search.settings import settings
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    KnowledgeboxCounters,
    KnowledgeboxShards,
    SearchParamDefaults,
)
from nucliadb_telemetry import errors
from nucliadb_utils.authentication import requires, requires_one
from nucliadb_utils.cache import KB_COUNTER_CACHE
from nucliadb_utils.utilities import get_cache


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/shards",
    status_code=200,
    description="Show shards from a knowledgebox",
    response_model=KnowledgeboxShards,
    include_in_schema=False,
    tags=["Knowledge Boxes"],
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def knowledgebox_shards(request: Request, kbid: str) -> KnowledgeboxShards:
    shard_manager = get_shard_manager()
    try:
        shards: Shards = await shard_manager.get_shards_by_kbid_inner(kbid)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )
    return KnowledgeboxShards.from_message(shards)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/counters",
    status_code=200,
    description="Summary of amount of different things inside a knowledgebox",
    response_model=KnowledgeboxCounters,
    tags=["Knowledge Boxes"],
    response_model_exclude_unset=True,
)
@requires_one([NucliaDBRoles.READER, NucliaDBRoles.MANAGER])
@version(1)
async def knowledgebox_counters(
    request: Request,
    kbid: str,
    vectorset: str = fastapi_query(SearchParamDefaults.vectorset),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
) -> KnowledgeboxCounters:
    cache = await get_cache()

    if cache is not None:
        cached_counters = await cache.get(KB_COUNTER_CACHE.format(kbid=kbid))

        if cached_counters is not None:
            cached_counters_obj = json.loads(cached_counters)
            # In case shards get cached, we don't want it to be retrieved if we are not debugging
            if not debug:
                cached_counters_obj.pop("shards", None)
            return KnowledgeboxCounters.parse_obj(cached_counters_obj)

    shard_manager = get_shard_manager()

    try:
        shard_groups: List[PBShardObject] = await shard_manager.get_shards_by_kbid(kbid)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )

    ops = []
    queried_shards = []
    for shard_object in shard_groups:
        try:
            node, shard_id, _ = choose_node(shard_object)
        except KeyError:
            raise HTTPException(
                status_code=500,
                detail="Couldn't retrieve counters right now, node not found",
            )
        else:
            if shard_id is not None:
                # At least one node is alive for this shard group
                # let's add it ot the query list if has a valid value
                ops.append(get_shard(node, shard_id, vectorset=vectorset))
                queried_shards.append(shard_id)

    if not ops:
        await abort_transaction()
        logger.info(f"No node found for any of this resources shards {kbid}")
        raise HTTPException(
            status_code=500,
            detail=f"No node found for any of this resources shards {kbid}",
        )

    try:
        results: Optional[List[Shard]] = await asyncio.wait_for(  # type: ignore
            asyncio.gather(*ops, return_exceptions=True),  # type: ignore
            timeout=settings.search_timeout,
        )
    except asyncio.TimeoutError as exc:
        errors.capture_exception(exc)
        await abort_transaction()
        raise HTTPException(status_code=503, detail=f"Data query took too long")
    except AioRpcError as exc:
        if exc.code() is GrpcStatusCode.UNAVAILABLE:
            raise HTTPException(status_code=503, detail=f"Search backend not available")
        else:
            raise exc

    if results is None:
        raise HTTPException(status_code=503, detail=f"No shards found")

    field_count = 0
    paragraph_count = 0
    sentence_count = 0

    for shard in results:
        if isinstance(shard, Exception):
            errors.capture_exception(shard)
            await abort_transaction()
            raise HTTPException(
                status_code=500, detail=f"Error while geting shard data"
            )

        field_count += shard.fields
        paragraph_count += shard.paragraphs
        sentence_count += shard.sentences

    # Get counters from maindb
    driver = get_driver()
    txn = await driver.begin()

    try:
        resource_count = 0
        async for _ in txn.keys(
            match=KB_RESOURCE_SLUG_BASE.format(kbid=kbid), count=-1
        ):
            resource_count += 1
    except Exception as exc:
        errors.capture_exception(exc)
        raise HTTPException(
            status_code=500, detail="Couldn't retrieve counters right now"
        )
    finally:
        await txn.abort()

    counters = KnowledgeboxCounters(
        resources=resource_count,
        paragraphs=paragraph_count,
        fields=field_count,
        sentences=sentence_count,
    )

    if debug:
        counters.shards = queried_shards
    if cache is not None:
        await cache.set(KB_COUNTER_CACHE.format(kbid=kbid), counters.json())
    return counters
