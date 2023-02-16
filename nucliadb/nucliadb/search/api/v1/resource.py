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
from nucliadb_protos.nodereader_pb2 import ParagraphSearchResponse
from sentry_sdk import capture_exception

from nucliadb.ingest.serialize import get_resource_uuid_by_slug
from nucliadb.search import SERVICE_NAME, logger
from nucliadb.search.search.fetch import abort_transaction
from nucliadb.search.search.merge import merge_paragraphs_results
from nucliadb.search.search.query import paragraph_query_to_pb
from nucliadb.search.search.shards import query_paragraph_shard
from nucliadb.search.settings import settings
from nucliadb.search.utilities import get_nodes
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import (
    NucliaDBClientType,
    ResourceProperties,
    ResourceSearchResults,
    SearchOptions,
    SortField,
)
from nucliadb_utils.authentication import requires_one
from nucliadb_utils.exceptions import ShardsNotFound

from .router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/search",
    status_code=200,
    description="Search on a Resource",
    tags=["Search"],
    response_model_exclude_unset=True,
)
@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/search",
    status_code=200,
    description="Search on a Resource",
    tags=["Search"],
    response_model_exclude_unset=True,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def search(
    request: Request,
    response: Response,
    kbid: str,
    query: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
    fields: List[str] = Query(default=[]),
    filters: List[str] = Query(default=[]),
    faceted: List[str] = Query(default=[]),
    sort: Optional[SortField] = None,
    page_number: int = 0,
    page_size: int = 20,
    range_creation_start: Optional[datetime] = None,
    range_creation_end: Optional[datetime] = None,
    range_modification_start: Optional[datetime] = None,
    range_modification_end: Optional[datetime] = None,
    reload: bool = Query(False),
    highlight: bool = Query(False),
    split: bool = Query(False),
    show: List[ResourceProperties] = Query(list(ResourceProperties)),
    field_type_filter: List[FieldTypeName] = Query(
        list(FieldTypeName), alias="field_type"
    ),
    extracted: List[ExtractedDataTypeName] = Query(list(ExtractedDataTypeName)),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    debug: bool = Query(False),
    shards: List[str] = Query(default=[]),
) -> ResourceSearchResults:
    if not rid:
        rid = await get_resource_uuid_by_slug(kbid, rslug, service_name=SERVICE_NAME)  # type: ignore
        if rid is None:
            raise HTTPException(status_code=404, detail="Resource does not exist")

    # We need the nodes/shards that are connected to the KB
    nodemanager = get_nodes()

    try:
        shard_groups = await nodemanager.get_shards_by_kbid(kbid)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )

    # We need to query all nodes
    pb_query = await paragraph_query_to_pb(
        [SearchOptions.PARAGRAPH],
        rid,
        query,
        fields,
        filters,
        faceted,
        page_number,
        page_size,
        range_creation_start,
        range_creation_end,
        range_modification_start,
        range_modification_end,
        reload=reload,
        sort=sort.value if sort else None,
    )

    incomplete_results = False
    ops = []
    queried_nodes = []
    queried_shards = []
    for shard_obj in shard_groups:
        try:
            node, shard_id, node_id = nodemanager.choose_node(shard_obj, shards)
        except KeyError:
            incomplete_results = True
        else:
            if shard_id is not None:
                # At least one node is alive for this shard group
                # let's add it ot the query list if has a valid value
                ops.append(query_paragraph_shard(node, shard_id, pb_query))
                queried_nodes.append((node.label, shard_id, node_id))
                queried_shards.append(shard_id)

    if not ops:
        await abort_transaction()
        raise HTTPException(
            status_code=500, detail=f"No node found for any of this resources shards"
        )

    try:
        results: Optional[List[ParagraphSearchResponse]] = await asyncio.wait_for(
            asyncio.gather(*ops, return_exceptions=True),  # type: ignore
            timeout=settings.search_timeout,  # type: ignore
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
    search_results = await merge_paragraphs_results(
        results,
        count=page_size,
        page=page_number,
        kbid=kbid,
        show=show,
        field_type_filter=field_type_filter,
        extracted=extracted,
        highlight_split=highlight,
    )
    await abort_transaction()

    response.status_code = 206 if incomplete_results else 200
    if debug:
        search_results.nodes = queried_nodes

    search_results.shards = queried_shards
    return search_results
