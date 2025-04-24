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
from typing import Optional, Union, cast

from fastapi import Header, Request, Response
from fastapi_versioning import version
from pydantic import ValidationError

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search import cache
from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb.search.search.merge import merge_paragraphs_results
from nucliadb.search.search.query import paragraph_query_to_pb
from nucliadb_models.filters import FilterExpression
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    NucliaDBClientType,
    ResourceSearchResults,
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
    filter_expression: Optional[str] = fastapi_query(SearchParamDefaults.filter_expression),
    fields: list[str] = fastapi_query(SearchParamDefaults.fields),
    filters: list[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: list[str] = fastapi_query(SearchParamDefaults.faceted),
    sort: Optional[SortField] = fastapi_query(SearchParamDefaults.sort_field, alias="sort_field"),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
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
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
) -> Union[ResourceSearchResults, HTTPClientError]:
    top_k = top_k or SearchParamDefaults.top_k  # type: ignore
    top_k = cast(int, top_k)

    with cache.request_caches():
        try:
            expr = FilterExpression.model_validate_json(filter_expression) if filter_expression else None

            pb_query = await paragraph_query_to_pb(
                kbid,
                rid,
                query,
                expr,
                fields,
                filters,
                faceted,
                top_k,
                range_creation_start,
                range_creation_end,
                range_modification_start,
                range_modification_end,
                sort=sort.value if sort else None,
                sort_ord=sort_order.value,
            )
        except InvalidQueryError as exc:
            return HTTPClientError(status_code=412, detail=str(exc))
        except ValidationError as exc:
            detail = json.loads(exc.json())
            return HTTPClientError(status_code=422, detail=detail)

        results, incomplete_results, queried_shards = await node_query(kbid, Method.SEARCH, pb_query)

        # We need to merge
        search_results = await merge_paragraphs_results(
            results,
            top_k=top_k,
            kbid=kbid,
            highlight_split=highlight,
            min_score=0.0,
        )

        response.status_code = 206 if incomplete_results else 200

        search_results.shards = queried_shards
        return search_results
