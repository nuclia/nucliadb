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
from time import time
from typing import Optional, Union

from fastapi import Body, Header, Query, Request, Response
from fastapi.openapi.models import Example
from fastapi_versioning import version
from pydantic import ValidationError

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.models_utils import to_proto
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb.search.search import cache
from nucliadb.search.search.merge import merge_results
from nucliadb.search.search.query_parser.parsers.search import parse_search
from nucliadb.search.search.query_parser.parsers.unit_retrieval import legacy_convert_retrieval_to_proto
from nucliadb.search.search.utils import (
    min_score_from_query_params,
)
from nucliadb_models.common import FieldTypeName
from nucliadb_models.filters import FilterExpression
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
from nucliadb_models.security import RequestSecurity
from nucliadb_models.utils import DateTime
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError
from nucliadb_utils.utilities import get_audit

SEARCH_EXAMPLES = {
    "filtering_by_icon": Example(
        summary="Search for pdf documents where the text 'Noam Chomsky' appears",
        description="For a complete list of filters, visit: https://github.com/nuclia/nucliadb/blob/main/docs/internal/SEARCH.md#filters-and-facets",  # noqa
        value={
            "query": "Noam Chomsky",
            "filters": ["/icon/application/pdf"],
            "features": [SearchOptions.FULLTEXT],
        },
    ),
    "get_language_counts": Example(
        summary="Get the number of documents for each language",
        description="For a complete list of facets, visit: https://github.com/nuclia/nucliadb/blob/main/docs/internal/SEARCH.md#filters-and-facets",  # noqa
        value={
            "page_size": 0,
            "faceted": ["/s/p"],
            "features": [SearchOptions.FULLTEXT],
        },
    ),
}


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/search",
    status_code=200,
    summary="Search Knowledge Box",
    description="Search on a Knowledge Box and retrieve separate results for documents, paragraphs, and sentences. Usually, it is better to use `find`",  # noqa: E501
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
    filter_expression: Optional[str] = fastapi_query(SearchParamDefaults.filter_expression),
    fields: list[str] = fastapi_query(SearchParamDefaults.fields),
    filters: list[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: list[str] = fastapi_query(SearchParamDefaults.faceted),
    sort_field: SortField = fastapi_query(SearchParamDefaults.sort_field),
    sort_limit: Optional[int] = fastapi_query(SearchParamDefaults.sort_limit),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
    top_k: int = fastapi_query(SearchParamDefaults.top_k),
    min_score: Optional[float] = Query(
        default=None,
        description="Minimum similarity score to filter vector index results. If not specified, the default minimum score of the semantic model associated to the Knowledge Box will be used. Check out the documentation for more information on how to use this parameter: https://docs.nuclia.dev/docs/rag/advanced/search#minimum-score",  # noqa: E501
        deprecated=True,
    ),
    min_score_semantic: Optional[float] = Query(
        default=None,
        description="Minimum semantic similarity score to filter vector index results. If not specified, the default minimum score of the semantic model associated to the Knowledge Box will be used. Check out the documentation for more information on how to use this parameter: https://docs.nuclia.dev/docs/rag/advanced/search#minimum-score",  # noqa: E501
    ),
    min_score_bm25: float = Query(
        default=0,
        description="Minimum bm25 score to filter paragraph and document index results",
        ge=0,
    ),
    vectorset: Optional[str] = fastapi_query(SearchParamDefaults.vectorset),
    range_creation_start: Optional[DateTime] = fastapi_query(SearchParamDefaults.range_creation_start),
    range_creation_end: Optional[DateTime] = fastapi_query(SearchParamDefaults.range_creation_end),
    range_modification_start: Optional[DateTime] = fastapi_query(
        SearchParamDefaults.range_modification_start
    ),
    range_modification_end: Optional[DateTime] = fastapi_query(
        SearchParamDefaults.range_modification_end
    ),
    features: list[SearchOptions] = fastapi_query(
        SearchParamDefaults.search_features,
        default=[
            SearchOptions.KEYWORD,
            SearchOptions.FULLTEXT,
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
    show_hidden: bool = fastapi_query(SearchParamDefaults.show_hidden),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    try:
        expr = FilterExpression.model_validate_json(filter_expression) if filter_expression else None

        security = None
        if len(security_groups) > 0:
            security = RequestSecurity(groups=security_groups)
        item = SearchRequest(
            query=query,
            filter_expression=expr,
            fields=fields,
            filters=filters,
            faceted=faceted,
            sort=(
                SortOptions(field=sort_field, limit=sort_limit, order=sort_order)
                if sort_field is not None
                else None
            ),
            top_k=top_k,
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
            show_hidden=show_hidden,
        )
    except ValidationError as exc:
        detail = json.loads(exc.json())
        return HTTPClientError(status_code=422, detail=detail)
    return await _search_endpoint(response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/search",
    status_code=200,
    summary="Search Knowledge Box",
    description="Search on a Knowledge Box and retrieve separate results for documents, paragraphs, and sentences. Usually, it is better to use `find`",  # noqa: E501
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
    item: SearchRequest = Body(openapi_examples=SEARCH_EXAMPLES),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    return await _search_endpoint(response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for)


async def _search_endpoint(
    response: Response,
    kbid: str,
    item: SearchRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    **kwargs,
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    try:
        with cache.request_caches():
            results, incomplete = await search(
                kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for, **kwargs
            )
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


async def search(
    kbid: str,
    item: SearchRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    do_audit: bool = True,
    with_status: Optional[ResourceProcessingStatus] = None,
) -> tuple[KnowledgeboxSearchResults, bool]:
    audit = get_audit()
    start_time = time()

    parsed = await parse_search(kbid, item)
    pb_query, incomplete_results, autofilters, _ = await legacy_convert_retrieval_to_proto(parsed)

    # We need to query all nodes
    results, queried_shards = await nidx_query(kbid, Method.SEARCH, pb_query)

    # We need to merge
    search_results = await merge_results(
        results,
        parsed.retrieval,
        kbid=kbid,
        show=item.show,
        field_type_filter=item.field_type_filter,
        extracted=item.extracted,
        highlight=item.highlight,
    )

    if audit is not None and do_audit:
        audit.search(
            kbid,
            x_nucliadb_user,
            to_proto.client_type(x_ndb_client),
            x_forwarded_for,
            pb_query,
            time() - start_time,
            len(search_results.resources),
        )

    search_results.shards = queried_shards
    search_results.autofilters = autofilters
    return search_results, incomplete_results
