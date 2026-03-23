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

from fastapi import Request, Response
from fastapi_versioning import version
from pydantic import ValidationError

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb.search.search import cache
from nucliadb.search.search.merge import merge_suggest_results
from nucliadb.search.search.query_parser.parsers import parse_suggest
from nucliadb_models import SuggestRequest
from nucliadb_models.filters import FilterExpression
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import KnowledgeboxSuggestResults
from nucliadb_models.suggest import SuggestOptions, SuggestParamDefaults
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
    query: str = fastapi_query(SuggestParamDefaults.suggest_query),
    filter_expression: str | None = fastapi_query(
        SuggestParamDefaults.filter_expression, include_in_schema=False
    ),
    fields: list[str] = fastapi_query(SuggestParamDefaults.fields),
    filters: list[str] = fastapi_query(SuggestParamDefaults.filters),
    range_creation_start: DateTime | None = fastapi_query(SuggestParamDefaults.range_creation_start),
    range_creation_end: DateTime | None = fastapi_query(SuggestParamDefaults.range_creation_end),
    range_modification_start: DateTime | None = fastapi_query(
        SuggestParamDefaults.range_modification_start
    ),
    range_modification_end: DateTime | None = fastapi_query(SuggestParamDefaults.range_modification_end),
    features: list[SuggestOptions] = fastapi_query(SuggestParamDefaults.suggest_features),
    debug: bool = fastapi_query(SuggestParamDefaults.debug),
    highlight: bool = fastapi_query(SuggestParamDefaults.highlight),
    show_hidden: bool = fastapi_query(SuggestParamDefaults.show_hidden),
    security_groups: list[str] = fastapi_query(SuggestParamDefaults.security_groups),
) -> KnowledgeboxSuggestResults | HTTPClientError:
    try:
        expr = FilterExpression.model_validate_json(filter_expression) if filter_expression else None

        return await suggest(
            response,
            kbid,
            query,
            expr,
            fields,
            filters,
            range_creation_start,
            range_creation_end,
            range_modification_start,
            range_modification_end,
            features,
            debug,
            highlight,
            show_hidden,
            security_groups,
        )
    except InvalidQueryError as exc:
        return HTTPClientError(status_code=412, detail=str(exc))
    except ValidationError as exc:
        detail = json.loads(exc.json())
        return HTTPClientError(status_code=422, detail=detail)


@api.post(
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
async def suggest_post_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    item: SuggestRequest,
) -> KnowledgeboxSuggestResults | HTTPClientError:
    try:
        return await suggest(
            response,
            kbid,
            query=item.query,
            filter_expression=item.filter_expression,
            fields=[],
            filters=[],
            range_creation_start=None,
            range_creation_end=None,
            range_modification_start=None,
            range_modification_end=None,
            features=item.features,
            debug=item.debug,
            highlight=item.highlight,
            show_hidden=item.show_hidden,
            security_groups=item.security.groups if item.security else [],
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
    filter_expression: FilterExpression | None,
    fields: list[str],
    filters: list[str],
    range_creation_start: datetime | None,
    range_creation_end: datetime | None,
    range_modification_start: datetime | None,
    range_modification_end: datetime | None,
    features: list[SuggestOptions],
    debug: bool,
    highlight: bool,
    show_hidden: bool,
    security_groups: list[str],
) -> KnowledgeboxSuggestResults:
    with cache.request_caches():
        pb_query = await parse_suggest(
            kbid,
            query,
            features,
            filter_expression,
            fields,
            filters,
            show_hidden,
            range_creation_start,
            range_creation_end,
            range_modification_start,
            range_modification_end,
            security_groups,
        )
        results, queried_shards = await nidx_query(kbid, Method.SUGGEST, pb_query)

        # We need to merge
        search_results = await merge_suggest_results(
            results,
            kbid=kbid,
            highlight=highlight,
        )
        if debug and queried_shards:
            search_results.shards = queried_shards

        return search_results
