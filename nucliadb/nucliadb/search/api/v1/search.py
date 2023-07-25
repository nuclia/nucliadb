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
import json
from datetime import datetime
from time import time
from typing import List, Optional, Tuple, Union

from fastapi import Body, Header, Request, Response
from fastapi_versioning import version
from pydantic.error_wrappers import ValidationError

from nucliadb.ingest.orm.exceptions import KnowledgeBoxNotFound
from nucliadb.ingest.txn_utils import abort_transaction
from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.merge import merge_results
from nucliadb.search.search.query import (
    get_default_min_score,
    global_query_to_pb,
    pre_process_query,
)
from nucliadb.search.search.utils import (
    parse_sort_options,
    should_disable_vector_search,
)
from nucliadb_models.common import FieldTypeName
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import (
    KnowledgeboxSearchResults,
    NucliaDBClientType,
    ResourceProperties,
    SearchOptions,
    SearchParamDefaults,
    SearchRequest,
    SortField,
    SortOptions,
    SortOrder,
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
    query: str = fastapi_query(SearchParamDefaults.query),
    fields: List[str] = fastapi_query(SearchParamDefaults.fields),
    filters: List[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: List[str] = fastapi_query(SearchParamDefaults.faceted),
    sort_field: SortField = fastapi_query(SearchParamDefaults.sort_field),
    sort_limit: Optional[int] = fastapi_query(SearchParamDefaults.sort_limit),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
    page_number: int = fastapi_query(SearchParamDefaults.page_number),
    page_size: int = fastapi_query(SearchParamDefaults.page_size),
    min_score: float = fastapi_query(SearchParamDefaults.min_score),
    range_creation_start: Optional[datetime] = fastapi_query(
        SearchParamDefaults.range_creation_start
    ),
    range_creation_end: Optional[datetime] = fastapi_query(
        SearchParamDefaults.range_creation_end
    ),
    range_modification_start: Optional[datetime] = fastapi_query(
        SearchParamDefaults.range_modification_start
    ),
    range_modification_end: Optional[datetime] = fastapi_query(
        SearchParamDefaults.range_modification_end
    ),
    features: List[SearchOptions] = fastapi_query(
        SearchParamDefaults.search_features,
        default=[
            SearchOptions.PARAGRAPH,
            SearchOptions.DOCUMENT,
            SearchOptions.VECTOR,
        ],
    ),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
    highlight: bool = fastapi_query(SearchParamDefaults.highlight),
    show: List[ResourceProperties] = fastapi_query(SearchParamDefaults.show),
    field_type_filter: List[FieldTypeName] = fastapi_query(
        SearchParamDefaults.field_type_filter, alias="field_type"
    ),
    extracted: List[ExtractedDataTypeName] = fastapi_query(
        SearchParamDefaults.extracted
    ),
    shards: List[str] = fastapi_query(SearchParamDefaults.shards),
    with_duplicates: bool = fastapi_query(SearchParamDefaults.with_duplicates),
    with_synonyms: bool = fastapi_query(SearchParamDefaults.with_synonyms),
    autofilter: bool = fastapi_query(SearchParamDefaults.autofilter),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    try:
        item = SearchRequest(
            query=query,
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
            debug=debug,
            highlight=highlight,
            show=show,
            field_type_filter=field_type_filter,
            extracted=extracted,
            shards=shards,
            with_duplicates=with_duplicates,
            with_synonyms=with_synonyms,
            autofilter=autofilter,
        )
    except ValidationError as exc:
        detail = json.loads(exc.json())
        return HTTPClientError(status_code=422, detail=detail)
    return await _search_endpoint(
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
    query: str = fastapi_query(SearchParamDefaults.query),
    filters: List[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: List[str] = fastapi_query(SearchParamDefaults.faceted),
    sort_field: SortField = fastapi_query(SearchParamDefaults.sort_field),
    sort_limit: Optional[int] = fastapi_query(SearchParamDefaults.sort_limit),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
    page_number: int = fastapi_query(SearchParamDefaults.page_number),
    page_size: int = fastapi_query(SearchParamDefaults.page_size),
    shards: List[str] = fastapi_query(SearchParamDefaults.shards),
    with_status: Optional[ResourceProcessingStatus] = fastapi_query(
        SearchParamDefaults.with_status
    ),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    try:
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
        )
    except ValidationError as exc:
        detail = json.loads(exc.json())
        return HTTPClientError(status_code=422, detail=detail)
    return await _search_endpoint(
        response,
        kbid,
        item,
        x_ndb_client,
        x_nucliadb_user,
        x_forwarded_for,
        do_audit=False,
        with_status=with_status,
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
    return await _search_endpoint(
        response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
    )


async def _search_endpoint(
    response: Response,
    kbid: str,
    item: SearchRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    **kwargs,
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    # All endpoint logic should be here
    try:
        results, incomplete = await search(
            kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for, **kwargs
        )
        response.status_code = 206 if incomplete else 200
        return results
    except KnowledgeBoxNotFound:
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)


async def search(
    kbid: str,
    item: SearchRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    do_audit: bool = True,
    with_status: Optional[ResourceProcessingStatus] = None,
) -> Tuple[KnowledgeboxSearchResults, bool]:
    audit = get_audit()
    start_time = time()

    sort_options = parse_sort_options(item)

    if SearchOptions.VECTOR in item.features:
        if should_disable_vector_search(item):
            item.features.remove(SearchOptions.VECTOR)

    min_score = item.min_score
    if min_score is None:
        min_score = await get_default_min_score(kbid)

    # We need to query all nodes
    processed_query = pre_process_query(item.query)
    pb_query, incomplete_results, autofilters = await global_query_to_pb(
        kbid,
        features=item.features,
        query=processed_query,
        filters=item.filters,
        faceted=item.faceted,
        sort=sort_options,
        page_number=item.page_number,
        page_size=item.page_size,
        min_score=min_score,
        range_creation_start=item.range_creation_start,
        range_creation_end=item.range_creation_end,
        range_modification_start=item.range_modification_start,
        range_modification_end=item.range_modification_end,
        fields=item.fields,
        user_vector=item.vector,
        vectorset=item.vectorset,
        with_duplicates=item.with_duplicates,
        with_status=with_status,
        with_synonyms=item.with_synonyms,
        autofilter=item.autofilter,
    )

    results, query_incomplete_results, queried_nodes, queried_shards = await node_query(
        kbid, Method.SEARCH, pb_query, item.shards
    )

    incomplete_results = incomplete_results or query_incomplete_results

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
        min_score=min_score,
        highlight=item.highlight,
    )
    await abort_transaction()

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
    search_results.autofilters = autofilters
    return search_results, incomplete_results
