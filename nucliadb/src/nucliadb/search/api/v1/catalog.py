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

from fastapi import Request, Response
from fastapi_versioning import version
from pydantic import ValidationError

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import logger
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.search import cache
from nucliadb.search.search.merge import fetch_resources
from nucliadb.search.search.pgcatalog import pgcatalog_facets, pgcatalog_search
from nucliadb.search.search.query_parser.parsers import parse_catalog
from nucliadb.search.search.utils import (
    maybe_log_request_payload,
)
from nucliadb_models.common import FieldTypeName
from nucliadb_models.filters import CatalogFilterExpression
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    CatalogFacetsRequest,
    CatalogFacetsResponse,
    CatalogRequest,
    CatalogResponse,
    KnowledgeboxSearchResults,
    ResourceProperties,
    SearchParamDefaults,
    SortField,
    SortOptions,
    SortOrder,
)
from nucliadb_models.utils import DateTime
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/catalog",
    status_code=200,
    summary="List resources of a Knowledge Box",
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
    filter_expression: Optional[str] = fastapi_query(SearchParamDefaults.catalog_filter_expression),
    filters: list[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: list[str] = fastapi_query(SearchParamDefaults.faceted),
    sort_field: SortField = fastapi_query(SearchParamDefaults.sort_field),
    sort_limit: Optional[int] = fastapi_query(SearchParamDefaults.sort_limit),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
    page_number: int = fastapi_query(SearchParamDefaults.catalog_page_number),
    page_size: int = fastapi_query(SearchParamDefaults.catalog_page_size),
    with_status: Optional[ResourceProcessingStatus] = fastapi_query(
        SearchParamDefaults.with_status, deprecated="Use filters instead"
    ),
    debug: bool = fastapi_query(SearchParamDefaults.debug, include_in_schema=False),
    range_creation_start: Optional[DateTime] = fastapi_query(SearchParamDefaults.range_creation_start),
    range_creation_end: Optional[DateTime] = fastapi_query(SearchParamDefaults.range_creation_end),
    range_modification_start: Optional[DateTime] = fastapi_query(
        SearchParamDefaults.range_modification_start
    ),
    range_modification_end: Optional[DateTime] = fastapi_query(
        SearchParamDefaults.range_modification_end
    ),
    hidden: Optional[bool] = fastapi_query(SearchParamDefaults.hidden),
    show: list[ResourceProperties] = fastapi_query(
        SearchParamDefaults.show, default=[ResourceProperties.BASIC, ResourceProperties.ERRORS]
    ),
) -> Union[CatalogResponse, HTTPClientError]:
    try:
        expr = (
            CatalogFilterExpression.model_validate_json(filter_expression) if filter_expression else None
        )
    except ValidationError as exc:
        detail = json.loads(exc.json())
        return HTTPClientError(status_code=422, detail=detail)

    item = CatalogRequest(
        query=query,
        filter_expression=expr,
        filters=filters,
        faceted=faceted,
        page_number=page_number,
        page_size=page_size,
        debug=debug,
        with_status=with_status,
        range_creation_start=range_creation_start,
        range_creation_end=range_creation_end,
        range_modification_start=range_modification_start,
        range_modification_end=range_modification_end,
        hidden=hidden,
        show=show,
    )
    if sort_field:
        item.sort = SortOptions(field=sort_field, limit=sort_limit, order=sort_order)
    return await catalog(kbid, item)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/catalog",
    status_code=200,
    summary="List resources of a Knowledge Box",
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
) -> Union[CatalogResponse, HTTPClientError]:
    return await catalog(kbid, item)


async def catalog(
    kbid: str,
    item: CatalogRequest,
) -> Union[HTTPClientError, CatalogResponse]:
    """
    Catalog endpoint is a simplified version of the search endpoint, it only
    returns bm25 results on titles and it does not support vector search.
    It is useful for listing resources in a knowledge box.
    """
    maybe_log_request_payload(kbid, "/catalog", item)
    start_time = time()
    try:
        with cache.request_caches():
            query_parser = await parse_catalog(kbid, item)

            catalog_results = CatalogResponse()
            catalog_results.fulltext = await pgcatalog_search(query_parser)
            catalog_results.resources = await fetch_resources(
                resources=[r.rid for r in catalog_results.fulltext.results],
                kbid=kbid,
                show=item.show,
                field_type_filter=list(FieldTypeName),
                extracted=[],
            )
            return catalog_results
    except InvalidQueryError as exc:
        return HTTPClientError(status_code=412, detail=str(exc))
    except KnowledgeBoxNotFound:
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
    finally:
        duration = time() - start_time
        max_time = 5 if item.faceted else 2
        if duration > max_time:  # pragma: no cover
            logger.warning(
                "Slow catalog request",
                extra={
                    "kbid": kbid,
                    "duration": duration,
                    "query": item.model_dump_json(),
                },
            )


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/catalog/facets",
    status_code=200,
    response_model=CatalogFacetsResponse,
    response_model_exclude_unset=True,
    tags=["Search"],
    summary="Show facets in Knowledge Box",
    description="List all facets in a Knowledge Box and how many resources they apply to",
    include_in_schema=False,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def catalog_facets(
    request: Request, kbid: str, item: CatalogFacetsRequest
) -> CatalogFacetsResponse:
    return CatalogFacetsResponse(facets=await pgcatalog_facets(kbid, item))
