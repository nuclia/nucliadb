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
from typing import Optional

from fastapi import HTTPException, Request
from fastapi_versioning import version
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError
from nidx_protos.noderesources_pb2 import Shard

from nucliadb.common import datamanagers
from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.constants import AVG_PARAGRAPH_SIZE_BYTES
from nucliadb.common.counters import IndexCounts
from nucliadb.common.external_index_providers.manager import get_external_index_manager
from nucliadb.common.models_utils import from_proto
from nucliadb.search import logger
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.search.shards import get_shard
from nucliadb.search.settings import settings
from nucliadb_models.internal.shards import KnowledgeboxShards
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    KnowledgeboxCounters,
    SearchParamDefaults,
)
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject
from nucliadb_protos.writer_pb2 import Shards
from nucliadb_telemetry import errors
from nucliadb_utils.authentication import requires, requires_one

MAX_PARAGRAPHS_FOR_SMALL_KB = 250_000


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
    return from_proto.kb_shards(shards)


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
    debug: bool = fastapi_query(SearchParamDefaults.debug),
) -> KnowledgeboxCounters:
    try:
        return await _kb_counters(kbid, debug=debug)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )


async def _kb_counters(
    kbid: str,
    debug: bool = False,
) -> KnowledgeboxCounters:
    """
    Resources count is calculated from maindb and cached
    Field count is calculated from the index node cluster
    Paragraphs and Sentences count is calculated from the index node cluster or the external index provider.
    Index size is estimated from the paragraphs count.
    """
    counters = KnowledgeboxCounters(
        resources=0,
        paragraphs=0,
        fields=0,
        sentences=0,
        index_size=0,
    )
    external_index_manager = await get_external_index_manager(kbid)
    if external_index_manager is not None:
        index_counts = await external_index_manager.get_index_counts()
        counters.paragraphs = index_counts.paragraphs
        counters.sentences = index_counts.sentences
        is_small_kb = index_counts.paragraphs < MAX_PARAGRAPHS_FOR_SMALL_KB
        resource_count = await get_resources_count(kbid, force_calculate=is_small_kb)
        # TODO: Find a way to query the fields count and size from the external index provider or use the catalog
        counters.resources = counters.fields = resource_count
        counters.index_size = counters.paragraphs * AVG_PARAGRAPH_SIZE_BYTES
    else:
        node_index_counts, queried_shards = await get_node_index_counts(kbid)
        counters.fields = node_index_counts.fields
        counters.paragraphs = node_index_counts.paragraphs
        counters.sentences = node_index_counts.sentences
        is_small_kb = node_index_counts.paragraphs < MAX_PARAGRAPHS_FOR_SMALL_KB
        resource_count = await get_resources_count(kbid, force_calculate=is_small_kb)
        counters.resources = resource_count
        counters.index_size = node_index_counts.size_bytes
    if debug and queried_shards is not None:
        counters.shards = queried_shards
    return counters


async def get_resources_count(kbid: str, force_calculate: bool = False) -> int:
    async with datamanagers.with_ro_transaction() as txn:
        if force_calculate:
            # For small kbs, this is faster and more up to date
            resource_count = await datamanagers.resources.calculate_number_of_resources(txn, kbid=kbid)
        else:
            resource_count = await datamanagers.resources.get_number_of_resources(txn, kbid=kbid)
            if resource_count == -1:
                # WARNING: standalone, this value will never be cached
                resource_count = await datamanagers.resources.calculate_number_of_resources(
                    txn, kbid=kbid
                )
    return resource_count


async def get_node_index_counts(kbid: str) -> tuple[IndexCounts, list[str]]:
    """
    Get the index counts for a knowledgebox that has an index in the index node cluster.
    """
    shard_manager = get_shard_manager()
    shard_groups: list[PBShardObject] = await shard_manager.get_shards_by_kbid(kbid)
    ops = []
    queried_shards = []
    for shard_object in shard_groups:
        shard_id = shard_object.nidx_shard_id
        if shard_id is not None:
            # At least one node is alive for this shard group
            # let's add it ot the query list if has a valid value
            ops.append(get_shard(shard_id))
            queried_shards.append(shard_id)

    if not ops:
        logger.info(f"No node found for any of this resources shards {kbid}")
        raise HTTPException(
            status_code=500,
            detail=f"No node found for any of this resources shards {kbid}",
        )

    try:
        results: Optional[list[Shard]] = await asyncio.wait_for(
            asyncio.gather(*ops, return_exceptions=True),  # type: ignore
            timeout=settings.search_timeout,
        )
    except asyncio.TimeoutError as exc:
        logger.exception("Timeout querying shards")
        errors.capture_exception(exc)
        raise HTTPException(status_code=503, detail=f"Data query took too long")
    except AioRpcError as exc:
        if exc.code() is GrpcStatusCode.UNAVAILABLE:
            raise HTTPException(status_code=503, detail=f"Search backend not available")
        else:
            raise exc

    if results is None:
        raise HTTPException(status_code=503, detail=f"No shards found")

    counts = IndexCounts(fields=0, paragraphs=0, sentences=0, size_bytes=0)
    for shard in results:
        if isinstance(shard, Exception):
            logger.error("Error getting shard info", exc_info=shard)
            errors.capture_exception(shard)
            raise HTTPException(status_code=500, detail=f"Error while geting shard data")
        counts.fields += shard.fields
        counts.paragraphs += shard.paragraphs
        counts.sentences += shard.sentences
        counts.size_bytes += shard.size_bytes
    return counts, queried_shards
