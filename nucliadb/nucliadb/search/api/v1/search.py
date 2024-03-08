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
from typing import Optional, Union

from fastapi import Body, Header, Query, Request, Response
from fastapi.openapi.models import Example
from fastapi_versioning import version
from pydantic.error_wrappers import ValidationError

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, debug_nodes_info, node_query
from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb.search.search.merge import merge_results
from nucliadb.search.search.query import QueryParser
from nucliadb.search.search.utils import (
    min_score_from_payload,
    min_score_from_query_params,
    should_disable_vector_search,
)
from nucliadb_models.common import FieldTypeName
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import (
    CatalogRequest,
    KnowledgeboxSearchResults,
    MinScore,
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
            "features": [SearchOptions.DOCUMENT],
        },
    ),
    "get_language_counts": Example(
        summary="Get the number of documents for each language",
        description="For a complete list of facets, visit: https://github.com/nuclia/nucliadb/blob/main/docs/internal/SEARCH.md#filters-and-facets",  # noqa
        value={
            "page_size": 0,
            "faceted": ["/s/p"],
            "features": [SearchOptions.DOCUMENT],
        },
    ),
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
    fields: list[str] = fastapi_query(SearchParamDefaults.fields),
    filters: list[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: list[str] = fastapi_query(SearchParamDefaults.faceted),
    sort_field: SortField = fastapi_query(SearchParamDefaults.sort_field),
    sort_limit: Optional[int] = fastapi_query(SearchParamDefaults.sort_limit),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
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
    features: list[SearchOptions] = fastapi_query(
        SearchParamDefaults.search_features,
        default=[
            SearchOptions.PARAGRAPH,
            SearchOptions.DOCUMENT,
            SearchOptions.VECTOR,
        ],
    ),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
    highlight: bool = fastapi_query(SearchParamDefaults.highlight),
    show: list[ResourceProperties] = fastapi_query(SearchParamDefaults.show),
    field_type_filter: list[FieldTypeName] = fastapi_query(
        SearchParamDefaults.field_type_filter, alias="field_type"
    ),
    extracted: list[ExtractedDataTypeName] = fastapi_query(
        SearchParamDefaults.extracted
    ),
    shards: list[str] = fastapi_query(SearchParamDefaults.shards),
    with_duplicates: bool = fastapi_query(SearchParamDefaults.with_duplicates),
    with_synonyms: bool = fastapi_query(SearchParamDefaults.with_synonyms),
    autofilter: bool = fastapi_query(SearchParamDefaults.autofilter),
    security_groups: list[str] = fastapi_query(SearchParamDefaults.security_groups),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    try:
        security = None
        if len(security_groups) > 0:
            security = RequestSecurity(groups=security_groups)
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
            min_score=min_score_from_query_params(
                min_score_bm25, min_score_semantic, min_score
            ),
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
            security=security,
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
async def catalog_get(
    request: Request,
    response: Response,
    kbid: str,
    query: str = fastapi_query(SearchParamDefaults.query),
    filters: list[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: list[str] = fastapi_query(SearchParamDefaults.faceted),
    sort_field: SortField = fastapi_query(SearchParamDefaults.sort_field),
    sort_limit: Optional[int] = fastapi_query(SearchParamDefaults.sort_limit),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
    page_number: int = fastapi_query(SearchParamDefaults.page_number),
    page_size: int = fastapi_query(SearchParamDefaults.page_size),
    shards: list[str] = fastapi_query(SearchParamDefaults.shards),
    with_status: Optional[ResourceProcessingStatus] = fastapi_query(
        SearchParamDefaults.with_status
    ),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    item = CatalogRequest(
        query=query,
        filters=filters,
        faceted=faceted,
        page_number=page_number,
        page_size=page_size,
        shards=shards,
        debug=debug,
        with_status=with_status,
    )
    if sort_field:
        item.sort = SortOptions(field=sort_field, limit=sort_limit, order=sort_order)
    return await catalog(kbid, item)


@api.post(
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
async def catalog_post(
    request: Request,
    kbid: str,
    item: CatalogRequest,
) -> Union[KnowledgeboxSearchResults, HTTPClientError]:
    return await catalog(kbid, item)


async def catalog(
    kbid: str,
    item: CatalogRequest,
):
    """
    Catalog endpoint is a simplified version of the search endpoint, it only
    returns bm25 results on titles and it does not support vector search.
    It is useful for listing resources in a knowledge box.
    """
    try:
        sort = item.sort
        if item.sort is None:
            # By default we sort by creation date (most recent first)
            sort = SortOptions(
                field=SortField.CREATED,
                order=SortOrder.DESC,
                limit=None,
            )

        query_parser = QueryParser(
            kbid=kbid,
            features=[SearchOptions.DOCUMENT],
            query=item.query,
            filters=item.filters,
            faceted=item.faceted,
            sort=sort,
            page_number=item.page_number,
            page_size=item.page_size,
            min_score=MinScore(bm25=0, semantic=0),
            fields=["a/title"],
            with_status=item.with_status,
        )
        pb_query, _, _ = await query_parser.parse()

        (results, _, queried_nodes) = await node_query(
            kbid,
            Method.SEARCH,
            pb_query,
            target_shard_replicas=item.shards,
            # Catalog should not go to read replicas because we want it to be
            # consistent and most up to date results
            use_read_replica_nodes=False,
        )

        # We need to merge
        search_results = await merge_results(
            results,
            count=item.page_size,
            page=item.page_number,
            kbid=kbid,
            show=[ResourceProperties.BASIC],
            field_type_filter=[],
            extracted=[],
            sort=sort,
            requested_relations=pb_query.relation_subgraph,
            min_score=query_parser.min_score,
            highlight=False,
        )
        # We don't need sentences, paragraphs or relations on the catalog
        # response, so we set to None so that fastapi doesn't include them
        # in the response payload
        search_results.sentences = None
        search_results.paragraphs = None
        search_results.relations = None
        if item.debug:
            search_results.nodes = debug_nodes_info(queried_nodes)
        queried_shards = [shard_id for _, shard_id in queried_nodes]
        search_results.shards = queried_shards
        return search_results
    except InvalidQueryError as exc:
        return HTTPClientError(status_code=412, detail=str(exc))
    except KnowledgeBoxNotFound:
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)


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
    item: SearchRequest = Body(openapi_examples=SEARCH_EXAMPLES),
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
    except InvalidQueryError as exc:
        return HTTPClientError(status_code=412, detail=str(exc))
    except predict.ProxiedPredictAPIError as err:
        return HTTPClientError(
            status_code=503,
            detail=f"Inference service unavailable. {err.status}: {err.detail}",
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

    item.min_score = min_score_from_payload(item.min_score)

    if SearchOptions.VECTOR in item.features:
        if should_disable_vector_search(item):
            item.features.remove(SearchOptions.VECTOR)

    # We need to query all nodes
    query_parser = QueryParser(
        kbid=kbid,
        features=item.features,
        query=item.query,
        filters=item.filters,
        faceted=item.faceted,
        sort=item.sort,
        page_number=item.page_number,
        page_size=item.page_size,
        min_score=item.min_score,
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
        security=item.security,
    )
    pb_query, incomplete_results, autofilters = await query_parser.parse()

    results, query_incomplete_results, queried_nodes = await node_query(
        kbid, Method.SEARCH, pb_query, target_shard_replicas=item.shards
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
        sort=query_parser.sort,
        requested_relations=pb_query.relation_subgraph,
        min_score=query_parser.min_score,
        highlight=item.highlight,
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
        search_results.nodes = debug_nodes_info(queried_nodes)

    queried_shards = [shard_id for _, shard_id in queried_nodes]
    search_results.shards = queried_shards
    search_results.autofilters = autofilters
    return search_results, incomplete_results
