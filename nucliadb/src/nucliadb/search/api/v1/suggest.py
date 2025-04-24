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
from typing import Optional, Union

from fastapi import Header, Request, Response
from fastapi_versioning import version
from pydantic import ValidationError

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search import cache
from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb.search.search.merge import merge_suggest_results
from nucliadb.search.search.query import suggest_query_to_pb
from nucliadb.search.search.utils import filter_hidden_resources
from nucliadb_models.common import FieldTypeName
from nucliadb_models.filters import FilterExpression
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    KnowledgeboxSuggestResults,
    NucliaDBClientType,
    ResourceProperties,
    SearchParamDefaults,
    SuggestOptions,
)
from nucliadb_models.utils import DateTime
from nucliadb_utils.authentication import requires


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/suggest",
    status_code=200,
    summary="Suggest on a knowledge box",
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
    query: str = fastapi_query(SearchParamDefaults.suggest_query),
    filter_expression: Optional[str] = fastapi_query(
        SearchParamDefaults.filter_expression, include_in_schema=False
    ),
    fields: list[str] = fastapi_query(SearchParamDefaults.fields),
    filters: list[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: list[str] = fastapi_query(SearchParamDefaults.faceted),
    range_creation_start: Optional[DateTime] = fastapi_query(SearchParamDefaults.range_creation_start),
    range_creation_end: Optional[DateTime] = fastapi_query(SearchParamDefaults.range_creation_end),
    range_modification_start: Optional[DateTime] = fastapi_query(
        SearchParamDefaults.range_modification_start
    ),
    range_modification_end: Optional[DateTime] = fastapi_query(
        SearchParamDefaults.range_modification_end
    ),
    features: list[SuggestOptions] = fastapi_query(SearchParamDefaults.suggest_features),
    show: list[ResourceProperties] = fastapi_query(SearchParamDefaults.show),
    field_type_filter: list[FieldTypeName] = fastapi_query(
        SearchParamDefaults.field_type_filter, alias="field_type"
    ),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
    highlight: bool = fastapi_query(SearchParamDefaults.highlight),
    show_hidden: bool = fastapi_query(SearchParamDefaults.show_hidden),
) -> Union[KnowledgeboxSuggestResults, HTTPClientError]:
    try:
        expr = FilterExpression.model_validate_json(filter_expression) if filter_expression else None

        return await suggest(
            response,
            kbid,
            query,
            expr,
            fields,
            filters,
            faceted,
            range_creation_start,
            range_creation_end,
            range_modification_start,
            range_modification_end,
            features,
            show,
            field_type_filter,
            x_ndb_client,
            x_nucliadb_user,
            x_forwarded_for,
            debug,
            highlight,
            show_hidden,
        )
    except InvalidQueryError as exc:
        return HTTPClientError(status_code=412, detail=str(exc))
    except ValidationError as exc:
        detail = json.loads(exc.json())
        return HTTPClientError(status_code=422, detail=detail)


async def suggest(
    response,
    kbid: str,
    query: str,
    filter_expression: Optional[FilterExpression],
    fields: list[str],
    filters: list[str],
    faceted: list[str],
    range_creation_start: Optional[datetime],
    range_creation_end: Optional[datetime],
    range_modification_start: Optional[datetime],
    range_modification_end: Optional[datetime],
    features: list[SuggestOptions],
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    debug: bool,
    highlight: bool,
    show_hidden: bool,
) -> KnowledgeboxSuggestResults:
    with cache.request_caches():
        hidden = await filter_hidden_resources(kbid, show_hidden)
        pb_query = await suggest_query_to_pb(
            kbid,
            features,
            query,
            filter_expression,
            fields,
            filters,
            faceted,
            range_creation_start,
            range_creation_end,
            range_modification_start,
            range_modification_end,
            hidden,
        )
        results, incomplete_results, queried_shards = await node_query(kbid, Method.SUGGEST, pb_query)

        # We need to merge
        search_results = await merge_suggest_results(
            results,
            kbid=kbid,
            highlight=highlight,
        )

        response.status_code = 206 if incomplete_results else 200

        if debug and queried_shards:
            search_results.shards = queried_shards

        return search_results
