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

from fastapi import Body, Header, Query, Request, Response
from fastapi.openapi.models import Example
from fastapi_versioning import version
from pydantic import ValidationError

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb.search.search.find import find
from nucliadb.search.search.utils import min_score_from_query_params
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import (
    FindRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    ResourceProperties,
    SearchOptions,
    SearchParamDefaults,
)
from nucliadb_models.security import RequestSecurity
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError

FIND_EXAMPLES = {
    "find_hybrid_search": Example(
        summary="Do a hybrid search on a Knowledge Box",
        description="Perform a hybrid search that will return text and semantic results matching the query",
        value={
            "query": "How can I be an effective product manager?",
            "features": [SearchOptions.KEYWORD, SearchOptions.SEMANTIC],
        },
    )
}


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/find",
    status_code=200,
    summary="Find Knowledge Box",
    description="Find on a Knowledge Box",
    response_model=KnowledgeboxFindResults,
    response_model_exclude_unset=True,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def find_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    query: str = fastapi_query(SearchParamDefaults.query),
    fields: list[str] = fastapi_query(SearchParamDefaults.fields),
    filters: list[str] = fastapi_query(SearchParamDefaults.filters),
    page_number: int = fastapi_query(SearchParamDefaults.page_number),
    page_size: int = fastapi_query(SearchParamDefaults.page_size),
    min_score: Optional[float] = Query(
        default=None,
        description="Minimum similarity score to filter vector index results. If not specified, the default minimum score of the semantic model associated to the Knowledge Box will be used. Check out the documentation for more information on how to use this parameter: https://docs.nuclia.dev/docs/docs/using/search/#minimum-score",  # noqa: E501
        deprecated=True,
    ),
    min_score_semantic: Optional[float] = Query(
        default=None,
        description="Minimum semantic similarity score to filter vector index results. If not specified, the default minimum score of the semantic model associated to the Knowledge Box will be used. Check out the documentation for more information on how to use this parameter: https://docs.nuclia.dev/docs/docs/using/search/#minimum-score",  # noqa: E501
    ),
    min_score_bm25: float = Query(
        default=0,
        description="Minimum bm25 score to filter paragraph and document index results",
        ge=0,
    ),
    vectorset: Optional[str] = fastapi_query(SearchParamDefaults.vectorset),
    range_creation_start: Optional[datetime] = fastapi_query(SearchParamDefaults.range_creation_start),
    range_creation_end: Optional[datetime] = fastapi_query(SearchParamDefaults.range_creation_end),
    range_modification_start: Optional[datetime] = fastapi_query(
        SearchParamDefaults.range_modification_start
    ),
    range_modification_end: Optional[datetime] = fastapi_query(
        SearchParamDefaults.range_modification_end
    ),
    features: list[SearchOptions] = fastapi_query(
        SearchParamDefaults.search_features,
        default=[
            SearchOptions.KEYWORD,
            SearchOptions.SEMANTIC,
        ],
    ),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
    highlight: bool = fastapi_query(SearchParamDefaults.highlight),
    show: list[ResourceProperties] = fastapi_query(SearchParamDefaults.show),
    field_type_filter: list[FieldTypeName] = fastapi_query(
        SearchParamDefaults.field_type_filter, alias="field_type"
    ),
    extracted: list[ExtractedDataTypeName] = fastapi_query(SearchParamDefaults.extracted),
    with_duplicates: bool = fastapi_query(SearchParamDefaults.with_duplicates),
    with_synonyms: bool = fastapi_query(SearchParamDefaults.with_synonyms),
    autofilter: bool = fastapi_query(SearchParamDefaults.autofilter),
    security_groups: list[str] = fastapi_query(SearchParamDefaults.security_groups),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxFindResults, HTTPClientError]:
    try:
        security = None
        if len(security_groups) > 0:
            security = RequestSecurity(groups=security_groups)
        item = FindRequest(
            query=query,
            fields=fields,
            filters=filters,
            page_number=page_number,
            page_size=page_size,
            min_score=min_score_from_query_params(min_score_bm25, min_score_semantic, min_score),
            vectorset=vectorset,
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
            with_duplicates=with_duplicates,
            with_synonyms=with_synonyms,
            autofilter=autofilter,
            security=security,
        )
    except ValidationError as exc:
        detail = json.loads(exc.json())
        return HTTPClientError(status_code=422, detail=detail)

    return await _find_endpoint(response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/find",
    status_code=200,
    summary="Find Knowledge Box",
    description="Find on a Knowledge Box",
    response_model=KnowledgeboxFindResults,
    response_model_exclude_unset=True,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def find_post_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    item: FindRequest = Body(openapi_examples=FIND_EXAMPLES),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxFindResults, HTTPClientError]:
    return await _find_endpoint(response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for)


async def _find_endpoint(
    response: Response,
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
) -> Union[KnowledgeboxFindResults, HTTPClientError]:
    try:
        results, incomplete, _ = await find(kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for)
        response.status_code = 206 if incomplete else 200
        return results
    except KnowledgeBoxNotFound:
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
    except InvalidQueryError as exc:
        return HTTPClientError(status_code=412, detail=str(exc))
    except predict.ProxiedPredictAPIError as err:
        return HTTPClientError(
            status_code=err.status,
            detail=err.detail,
        )
