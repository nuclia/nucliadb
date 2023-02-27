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
from time import time
from typing import List, Optional

from fastapi import Body, Header, HTTPException, Query, Request, Response
from fastapi_versioning import version
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.nodereader_pb2 import SearchResponse
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject
from sentry_sdk import capture_exception

from nucliadb.search import logger
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search.fetch import abort_transaction  # type: ignore
from nucliadb.search.search.merge import merge_results
from nucliadb.search.search.query import global_query_to_pb, pre_process_query
from nucliadb.search.search.shards import query_shard
from nucliadb.search.settings import settings
from nucliadb.search.utilities import get_nodes
from nucliadb_models.common import FieldTypeName
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import (
    KnowledgeboxSearchResults,
    NucliaDBClientType,
    ResourceProperties,
    SearchOptions,
    SearchRequest,
    SortField,
    SortFieldMap,
    SortOptions,
    SortOrder,
    SortOrderMap,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import ShardsNotFound
from nucliadb_utils.utilities import get_audit

SEARCH_EXAMPLES = {
    "filtering_by_icon": {
        "summary": "Search for pdf documents where the text 'Noam Chomsky' appears",
        "description": "For a complete list of filters, visit: https://github.com/nuclia/nucliadb/blob/main/docs/internal/SEARCH.md#filters-and-facets",  # noqa
        "value": {
            "query": "Noam Chomsky",
            "filters": ["/n/i/application/pdf"],
            "features": [SearchOptions.DOCUMENT],
        },
    },
    "get_language_counts": {
        "summary": "Get the number of documents for each language",
        "description": "For a complete list of facets, visit: https://github.com/nuclia/nucliadb/blob/main/docs/internal/SEARCH.md#filters-and-facets",  # noqa
        "value": {
            "page_size": 0,
            "faceted": ["/s/p"],
            "features": [SearchOptions.DOCUMENT],
        },
    },
}


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/search",
    status_code=200,
    name="Search Knowledge Box",
    description="Search on a Knowledge Box",
    response_model=KnowledgeboxSearchResults,
    response_model_exclude_unset=True,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def search_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    query: str = Query(default=""),
    advanced_query: Optional[str] = Query(default=None),
    fields: List[str] = Query(default=[]),
    filters: List[str] = Query(default=[]),
    faceted: List[str] = Query(default=[]),
    sort_field: Optional[SortField] = Query(default=None),
    sort_limit: Optional[int] = Query(default=None, gt=0),
    sort_order: SortOrder = Query(default=SortOrder.DESC),
    page_number: int = Query(default=0),
    page_size: int = Query(default=20),
    min_score: float = Query(default=0.70),
    range_creation_start: Optional[datetime] = Query(default=None),
    range_creation_end: Optional[datetime] = Query(default=None),
    range_modification_start: Optional[datetime] = Query(default=None),
    range_modification_end: Optional[datetime] = Query(default=None),
    features: List[SearchOptions] = Query(
        default=[
            SearchOptions.PARAGRAPH,
            SearchOptions.DOCUMENT,
            SearchOptions.VECTOR,
        ]
    ),
    reload: bool = Query(default=True),
    debug: bool = Query(False),
    highlight: bool = Query(default=False),
    show: List[ResourceProperties] = Query([ResourceProperties.BASIC]),
    field_type_filter: List[FieldTypeName] = Query(
        list(FieldTypeName), alias="field_type"
    ),
    extracted: List[ExtractedDataTypeName] = Query(list(ExtractedDataTypeName)),
    shards: List[str] = Query([]),
    with_duplicates: bool = Query(default=False),
    with_status: Optional[ResourceProcessingStatus] = Query(default=None),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> KnowledgeboxSearchResults:
    item = SearchRequest(
        query=query,
        advanced_query=advanced_query,
        fields=fields,
        filters=filters,
        faceted=faceted,
        sort=(
            SortOptions(field=sort_field, limit=sort_limit, order=sort_order)
            if sort_field is not None
            else None
        ),
        page_number=page_number,
        page_size=page_size,
        min_score=min_score,
        range_creation_end=range_creation_end,
        range_creation_start=range_creation_start,
        range_modification_end=range_modification_end,
        range_modification_start=range_modification_start,
        features=features,
        reload=reload,
        debug=debug,
        highlight=highlight,
        show=show,
        field_type_filter=field_type_filter,
        extracted=extracted,
        shards=shards,
        with_duplicates=with_duplicates,
        with_status=with_status,
    )
    return await search(
        response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
    )


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/catalog",
    status_code=200,
    name="List resources of a Knowledge Box",
    description="List resources of a Knowledge Box",
    response_model=KnowledgeboxSearchResults,
    response_model_exclude_unset=True,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def catalog(
    request: Request,
    response: Response,
    kbid: str,
    query: str = Query(default=""),
    filters: List[str] = Query(default=[]),
    faceted: List[str] = Query(default=[]),
    sort_field: Optional[SortField] = Query(default=None),
    sort_limit: int = Query(default=None, gt=0),
    sort_order: SortOrder = Query(default=SortOrder.ASC),
    page_number: int = Query(default=0),
    page_size: int = Query(default=20),
    shards: List[str] = Query([]),
    with_status: Optional[ResourceProcessingStatus] = Query(default=None),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> KnowledgeboxSearchResults:
    sort = None
    if sort_field:
        sort = SortOptions(field=sort_field, limit=sort_limit, order=sort_order)
    item = SearchRequest(
        query=query,
        fields=["a/title"],
        faceted=faceted,
        filters=filters,
        sort=sort,
        page_number=page_number,
        page_size=page_size,
        features=[SearchOptions.DOCUMENT],
        show=[ResourceProperties.BASIC],
        shards=shards,
        with_status=with_status,
    )
    return await search(
        response,
        kbid,
        item,
        x_ndb_client,
        x_nucliadb_user,
        x_forwarded_for,
        do_audit=False,
    )


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/search",
    status_code=200,
    name="Search Knowledge Box",
    description="Search on a Knowledge Box",
    response_model=KnowledgeboxSearchResults,
    response_model_exclude_unset=True,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def search_post_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    item: SearchRequest = Body(examples=SEARCH_EXAMPLES),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> KnowledgeboxSearchResults:
    # We need the nodes/shards that are connected to the KB
    return await search(
        response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
    )


async def search(
    response: Response,
    kbid: str,
    item: SearchRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    do_audit: bool = True,
) -> KnowledgeboxSearchResults:
    nodemanager = get_nodes()
    audit = get_audit()
    start_time = time()

    sort_options = parse_sort_options(item)

    if item.query == "" and (item.vector is None or len(item.vector) == 0):
        # If query is not defined we force to not return vector results
        if SearchOptions.VECTOR in item.features:
            item.features.remove(SearchOptions.VECTOR)

    try:
        shard_groups: List[PBShardObject] = await nodemanager.get_shards_by_kbid(kbid)
    except ShardsNotFound:
        raise HTTPException(
            status_code=404,
            detail="The knowledgebox or its shards configuration is missing",
        )

    # We need to query all nodes
    processed_query = pre_process_query(item.query)
    pb_query, incomplete_results = await global_query_to_pb(
        kbid,
        features=item.features,
        query=processed_query,
        advanced_query=item.advanced_query,
        filters=item.filters,
        faceted=item.faceted,
        sort=sort_options,
        sort_ord=SortOrderMap[sort_options.order],
        page_number=item.page_number,
        page_size=item.page_size,
        range_creation_start=item.range_creation_start,
        range_creation_end=item.range_creation_end,
        range_modification_start=item.range_modification_start,
        range_modification_end=item.range_modification_end,
        fields=item.fields,
        reload=item.reload,
        user_vector=item.vector,
        vectorset=item.vectorset,
        with_duplicates=item.with_duplicates,
        with_status=item.with_status,
    )

    ops = []
    queried_shards = []
    queried_nodes = []
    for shard_obj in shard_groups:
        try:
            node, shard_id, node_id = nodemanager.choose_node(shard_obj, item.shards)
        except KeyError:
            incomplete_results = True
        else:
            if shard_id is not None:
                # At least one node is alive for this shard group
                # let's add it ot the query list if has a valid value
                ops.append(query_shard(node, shard_id, pb_query))
                queried_nodes.append((node.label, shard_id, node_id))
                queried_shards.append(shard_id)

    if not ops:
        await abort_transaction()
        logger.info(f"No node found for any of this resources shards {kbid}")
        raise HTTPException(
            status_code=500,
            detail=f"No node found for any of this resources shards {kbid}",
        )

    try:
        results: Optional[List[SearchResponse]] = await asyncio.wait_for(  # type: ignore
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
    search_results = await merge_results(
        results,
        count=item.page_size,
        page=item.page_number,
        kbid=kbid,
        show=item.show,
        field_type_filter=item.field_type_filter,
        extracted=item.extracted,
        sort=sort_options,
        requested_relations=pb_query.relations,
        min_score=item.min_score,
        highlight=item.highlight,
    )
    await abort_transaction()

    response.status_code = 206 if incomplete_results else 200

    if audit is not None and do_audit:
        await audit.search(
            kbid,
            x_nucliadb_user,
            x_ndb_client.to_proto(),
            x_forwarded_for,
            pb_query,
            time() - start_time,
            len(search_results.resources),
        )
    if item.debug:
        search_results.nodes = queried_nodes

    search_results.shards = queried_shards
    return search_results


def parse_sort_options(item: SearchRequest) -> SortOptions:
    if is_empty_query(item):
        if item.sort is None:
            sort_options = SortOptions(
                field=SortField.CREATED,
                order=SortOrder.DESC,
                limit=None,
            )
        elif not is_valid_index_sort_field(item.sort.field):
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Empty query can only be sorted by '{SortField.CREATED}' or"
                    f" '{SortField.MODIFIED}' and sort limit won't be applied"
                ),
            )
        else:
            sort_options = item.sort
    else:
        if item.sort is None:
            sort_options = SortOptions(
                field=SortField.SCORE,
                order=SortOrder.DESC,
                limit=None,
            )
        elif not is_valid_index_sort_field(item.sort.field) and item.sort.limit is None:
            raise HTTPException(
                status_code=422,
                detail=f"Sort by '{item.sort.field}' requires setting a sort limit",
            )
        else:
            sort_options = item.sort

    return sort_options


def is_empty_query(request: SearchRequest) -> bool:
    return len(request.query) == 0 and (
        request.advanced_query is None or len(request.advanced_query) == 0
    )


def is_valid_index_sort_field(field: SortField) -> bool:
    return SortFieldMap[field] is not None
