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
from datetime import datetime
from time import time
from typing import List, Optional, Union

from fastapi import Body, Header, Query, Request, Response
from fastapi_versioning import version

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.fetch import abort_transaction  # type: ignore
from nucliadb.search.search.merge import merge_results
from nucliadb.search.search.query import global_query_to_pb, pre_process_query
from nucliadb.search.search.utils import parse_sort_options
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
    SortOptions,
    SortOrder,
    SortOrderMap,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError
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
    with_synonyms: bool = Query(default=False),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
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
        with_synonyms=with_synonyms,
    )
    try:
        return await search(
            response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
        )
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)


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
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    try:
        return await search(
            response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
        )
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)


async def search(
    response: Response,
    kbid: str,
    item: SearchRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    do_audit: bool = True,
) -> KnowledgeboxSearchResults:
    audit = get_audit()
    start_time = time()

    sort_options = parse_sort_options(item)

    if item.query == "" and (item.vector is None or len(item.vector) == 0):
        # If query is not defined we force to not return vector results
        if SearchOptions.VECTOR in item.features:
            item.features.remove(SearchOptions.VECTOR)

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
        with_synonyms=item.with_synonyms,
    )

    results, query_incomplete_results, queried_nodes, queried_shards = await node_query(
        kbid, Method.SEARCH, pb_query, item.shards
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
        requested_relations=pb_query.relation_subgraph,
        min_score=item.min_score,
        highlight=item.highlight,
    )
    await abort_transaction()

    response.status_code = (
        206 if incomplete_results or query_incomplete_results else 200
    )

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
