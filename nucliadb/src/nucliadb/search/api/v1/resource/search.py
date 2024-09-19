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
from typing import Optional, Union

from fastapi import Header, Request, Response
from fastapi_versioning import version

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, debug_nodes_info, node_query
from nucliadb.search.search import cache
from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb.search.search.merge import merge_paragraphs_results
from nucliadb.search.search.query import paragraph_query_to_pb
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import (
    NucliaDBClientType,
    ResourceProperties,
    ResourceSearchResults,
    SearchOptions,
    SearchParamDefaults,
    SortField,
    SortOrder,
)
from nucliadb_models.utils import DateTime
from nucliadb_utils.authentication import requires_one


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/search",
    status_code=200,
    summary="Search on Resource",
    description="Search on a single resource",
    tags=["Search"],
    response_model_exclude_unset=True,
    response_model=ResourceSearchResults,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def resource_search(
    request: Request,
    response: Response,
    kbid: str,
    query: str,
    rid: str,
    fields: list[str] = fastapi_query(SearchParamDefaults.fields),
    filters: list[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: list[str] = fastapi_query(SearchParamDefaults.faceted),
    sort: Optional[SortField] = fastapi_query(SearchParamDefaults.sort_field, alias="sort_field"),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
    page_number: int = fastapi_query(SearchParamDefaults.page_number),
    page_size: int = fastapi_query(SearchParamDefaults.page_size),
    top_k: Optional[int] = fastapi_query(SearchParamDefaults.top_k),
    range_creation_start: Optional[DateTime] = fastapi_query(SearchParamDefaults.range_creation_start),
    range_creation_end: Optional[DateTime] = fastapi_query(SearchParamDefaults.range_creation_end),
    range_modification_start: Optional[DateTime] = fastapi_query(
        SearchParamDefaults.range_modification_start
    ),
    range_modification_end: Optional[DateTime] = fastapi_query(
        SearchParamDefaults.range_modification_end
    ),
    highlight: bool = fastapi_query(SearchParamDefaults.highlight),
    show: list[ResourceProperties] = fastapi_query(
        SearchParamDefaults.show, default=list(ResourceProperties)
    ),
    field_type_filter: list[FieldTypeName] = fastapi_query(
        SearchParamDefaults.field_type_filter, alias="field_type"
    ),
    extracted: list[ExtractedDataTypeName] = fastapi_query(SearchParamDefaults.extracted),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
    shards: list[str] = fastapi_query(SearchParamDefaults.shards),
) -> Union[ResourceSearchResults, HTTPClientError]:
    if top_k is not None:
        page_number = 0
        page_size = top_k
    with cache.request_caches():
        try:
            pb_query = await paragraph_query_to_pb(
                kbid,
                [SearchOptions.KEYWORD],
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
                sort=sort.value if sort else None,
                sort_ord=sort_order.value,
            )
        except InvalidQueryError as exc:
            return HTTPClientError(status_code=412, detail=str(exc))

        results, incomplete_results, queried_nodes = await node_query(
            kbid, Method.PARAGRAPH, pb_query, shards
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
            min_score=0.0,
        )

        response.status_code = 206 if incomplete_results else 200
        if debug:
            search_results.nodes = debug_nodes_info(queried_nodes)

        queried_shards = [shard_id for _, shard_id in queried_nodes]
        search_results.shards = queried_shards
        return search_results
