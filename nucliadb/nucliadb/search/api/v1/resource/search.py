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
from typing import List, Optional

from fastapi import Header, HTTPException, Query, Request, Response
from fastapi_versioning import version

from nucliadb.ingest.serialize import get_resource_uuid_by_slug
from nucliadb.ingest.txn_utils import abort_transaction
from nucliadb.search import SERVICE_NAME
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, node_query
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
from nucliadb_utils.authentication import requires_one

from .router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/search",
    status_code=200,
    name="Search on Resource (by slug)",
    description="Search on a Resource",
    tags=["Search"],
    response_model_exclude_unset=True,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def search_by_rslug(
    request: Request,
    response: Response,
    kbid: str,
    rslug: str,
    query: str = fastapi_query(SearchParamDefaults.query),
    fields: List[str] = fastapi_query(SearchParamDefaults.fields),
    filters: List[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: List[str] = fastapi_query(SearchParamDefaults.faceted),
    sort: Optional[SortField] = fastapi_query(
        SearchParamDefaults.sort_field, alias="sort_field"
    ),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
    page_number: int = fastapi_query(SearchParamDefaults.page_number),
    page_size: int = fastapi_query(SearchParamDefaults.page_size),
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
    reload: bool = Query(False),
    highlight: bool = fastapi_query(SearchParamDefaults.highlight),
    show: List[ResourceProperties] = fastapi_query(
        SearchParamDefaults.show, default=list(ResourceProperties)
    ),
    field_type_filter: List[FieldTypeName] = fastapi_query(
        SearchParamDefaults.field_type_filter, alias="field_type"
    ),
    extracted: List[ExtractedDataTypeName] = fastapi_query(
        SearchParamDefaults.extracted
    ),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
    shards: List[str] = fastapi_query(SearchParamDefaults.shards),
) -> ResourceSearchResults:
    return await _search_on_resource(
        rid=None,
        rslug=rslug,
        response=response,
        kbid=kbid,
        query=query,
        fields=fields,
        filters=filters,
        faceted=faceted,
        sort=sort,
        sort_order=sort_order,
        page_number=page_number,
        page_size=page_size,
        range_creation_start=range_creation_start,
        range_creation_end=range_creation_end,
        range_modification_start=range_modification_start,
        range_modification_end=range_modification_end,
        reload=reload,
        highlight=highlight,
        show=show,
        field_type_filter=field_type_filter,
        extracted=extracted,
        x_ndb_client=x_ndb_client,
        debug=debug,
        shards=shards,
    )


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/search",
    status_code=200,
    name="Search on Resource (by id)",
    description="Search on a Resource",
    tags=["Search"],
    response_model_exclude_unset=True,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def search_by_rid(
    request: Request,
    response: Response,
    kbid: str,
    rid: str,
    query: str = fastapi_query(SearchParamDefaults.query),
    fields: List[str] = fastapi_query(SearchParamDefaults.fields),
    filters: List[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: List[str] = fastapi_query(SearchParamDefaults.faceted),
    sort: Optional[SortField] = fastapi_query(
        SearchParamDefaults.sort_field, alias="sort_field"
    ),
    sort_order: SortOrder = fastapi_query(SearchParamDefaults.sort_order),
    page_number: int = fastapi_query(SearchParamDefaults.page_number),
    page_size: int = fastapi_query(SearchParamDefaults.page_size),
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
    reload: bool = Query(False),
    highlight: bool = fastapi_query(SearchParamDefaults.highlight),
    show: List[ResourceProperties] = fastapi_query(
        SearchParamDefaults.show, default=list(ResourceProperties)
    ),
    field_type_filter: List[FieldTypeName] = fastapi_query(
        SearchParamDefaults.field_type_filter, alias="field_type"
    ),
    extracted: List[ExtractedDataTypeName] = fastapi_query(
        SearchParamDefaults.extracted
    ),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
    shards: List[str] = fastapi_query(SearchParamDefaults.shards),
) -> ResourceSearchResults:
    return await _search_on_resource(
        rid=rid,
        rslug=None,
        response=response,
        kbid=kbid,
        query=query,
        fields=fields,
        filters=filters,
        faceted=faceted,
        sort=sort,
        sort_order=sort_order,
        page_number=page_number,
        page_size=page_size,
        range_creation_start=range_creation_start,
        range_creation_end=range_creation_end,
        range_modification_start=range_modification_start,
        range_modification_end=range_modification_end,
        reload=reload,
        highlight=highlight,
        show=show,
        field_type_filter=field_type_filter,
        extracted=extracted,
        x_ndb_client=x_ndb_client,
        debug=debug,
        shards=shards,
    )


async def _search_on_resource(
    rid: Optional[str],
    rslug: Optional[str],
    response: Response,
    kbid: str,
    query: str,
    fields: List[str],
    filters: List[str],
    faceted: List[str],
    sort: Optional[SortField],
    sort_order: SortOrder,
    page_number: int,
    page_size: int,
    range_creation_start: Optional[datetime],
    range_creation_end: Optional[datetime],
    range_modification_start: Optional[datetime],
    range_modification_end: Optional[datetime],
    reload: bool,
    highlight: bool,
    show: List[ResourceProperties],
    field_type_filter: List[FieldTypeName],
    extracted: List[ExtractedDataTypeName],
    x_ndb_client: NucliaDBClientType,
    debug: bool,
    shards: List[str],
):
    if all([rid, rslug]) or not any([rid, rslug]):
        raise ValueError("Either rid or rslug must be provided")

    if not rid:
        rid = await get_resource_uuid_by_slug(kbid, rslug, service_name=SERVICE_NAME)  # type: ignore
        if rid is None:
            raise HTTPException(status_code=404, detail="Resource does not exist")

    # We need to query all nodes
    pb_query = await paragraph_query_to_pb(
        [SearchOptions.PARAGRAPH],
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
        reload=reload,
        sort=sort.value if sort else None,
        sort_ord=sort_order.value,
    )

    results, incomplete_results, queried_nodes, queried_shards = await node_query(
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
    )
    await abort_transaction()

    response.status_code = 206 if incomplete_results else 200
    if debug:
        search_results.nodes = queried_nodes

    search_results.shards = queried_shards
    return search_results
