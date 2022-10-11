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
from datetime import datetime
from typing import List, Optional

from fastapi import Header, HTTPException, Query, Request, Response
from fastapi_versioning import version
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.nodereader_pb2 import SuggestResponse
from nucliadb_protos.writer_pb2 import ShardObject
from sentry_sdk import capture_exception

from nucliadb.ingest.serialize import ResourceProperties
from nucliadb.models.common import FieldTypeName
from nucliadb.models.resource import NucliaDBRoles
from nucliadb.search import logger
from nucliadb.search.api.models import (
    KnowledgeboxSuggestResults,
    NucliaDBClientType,
    SuggestOptions,
)
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search.fetch import abort_transaction  # type: ignore
from nucliadb.search.search.merge import merge_suggest_results
from nucliadb.search.search.query import suggest_query_to_pb
from nucliadb.search.search.shards import suggest_shard
from nucliadb.search.settings import settings
from nucliadb.search.utilities import get_counter, get_nodes
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import ShardsNotFound


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/suggest",
    status_code=200,
    description="Suggestions on a knowledge box",
    response_model=KnowledgeboxSuggestResults,
    response_model_exclude_unset=True,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def suggest_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    query: str,
    fields: Optional[List[str]] = None,
    filters: Optional[List[str]] = None,
    faceted: Optional[List[str]] = None,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    features: List[SuggestOptions] = [
        SuggestOptions.PARAGRAPH,
        SuggestOptions.ENTITIES,
        SuggestOptions.INTENT,
    ],
    show: List[ResourceProperties] = Query([ResourceProperties.BASIC]),
    field_type_filter: List[FieldTypeName] = Query(
        list(FieldTypeName), alias="field_type"
    ),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
    debug: bool = Query(False),
    highlight: bool = Query(False),
) -> KnowledgeboxSuggestResults:
    filters = filters or []
    faceted = faceted or []

    # We need the nodes/shards that are connected to the KB
    nodemanager = get_nodes()

    try:
        shard_groups: List[ShardObject] = await nodemanager.get_shards_by_kbid(kbid)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )

    # We need to query all nodes
    pb_query = await suggest_query_to_pb(
        features,
        query,
        filters,
        faceted,
        range_creation_start,
        range_creation_end,
        range_modification_start,
        range_modification_end,
        fields=fields,
    )

    incomplete_results = False
    ops = []
    queried_shards = []
    for shard in shard_groups:
        try:
            node, shard_id, node_id = nodemanager.choose_node(shard)
        except KeyError:
            incomplete_results = True
        else:
            if shard_id is not None:
                # At least one node is alive for this shard group
                # let's add it ot the query list if has a valid value
                ops.append(suggest_shard(node, shard_id, pb_query))
                queried_shards.append((node.label, shard_id, node_id))

    if not ops:
        await abort_transaction()
        logger.info(f"No node found for any of this resources shards {kbid}")
        raise HTTPException(
            status_code=500,
            detail=f"No node found for any of this resources shards {kbid}",
        )

    try:
        results: Optional[List[SuggestResponse]] = await asyncio.wait_for(  # type: ignore
            asyncio.gather(*ops, return_exceptions=True),  # type: ignore
            timeout=settings.search_timeout,
        )
    except asyncio.TimeoutError as exc:
        capture_exception(exc)
        await abort_transaction()
        raise HTTPException(status_code=503, detail=f"Data query took too long")
    except AioRpcError as exc:
        if exc.code() is GrpcStatusCode.UNAVAILABLE:
            raise HTTPException(status_code=503, detail=f"Search backend not available")
        else:
            raise exc

    if results is None:
        await abort_transaction()
        raise HTTPException(
            status_code=500, detail=f"Error while executing shard queries"
        )

    for result in results:
        if isinstance(result, Exception):
            capture_exception(result)
            await abort_transaction()
            logger.exception("Error while querying shard data", exc_info=True)
            raise HTTPException(
                status_code=500, detail=f"Error while querying shard data"
            )

    # We need to merge
    search_results = await merge_suggest_results(
        results,
        kbid=kbid,
        show=show,
        field_type_filter=field_type_filter,
        highlight=highlight,
    )
    await abort_transaction()

    get_counter()[f"{kbid}_-_suggest_client_{x_ndb_client.value}"] += 1
    response.status_code = 206 if incomplete_results else 200
    if debug:
        search_results.shards = queried_shards
    return search_results
