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
from typing import List, Optional, Union

from fastapi import Body, Header, Query, Request, Response
from fastapi_versioning import version
from pydantic.error_wrappers import ValidationError

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import param_to_query
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.find_merge import find_merge_results
from nucliadb.search.search.query import global_query_to_pb, pre_process_query
from nucliadb.search.search.utils import parse_sort_options
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import (
    FindRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    ResourceProperties,
    SearchOptions,
    SearchParamDefaults,
    SortField,
    SortOptions,
    SortOrder,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError
from nucliadb_utils.utilities import get_audit

FIND_EXAMPLES = {
    "find_hybrid_search": {
        "summary": "Do a hybrid search on a Knowledge Box",
        "description": "Perform a hybrid search that will return text and semantic results matching the query",
        "value": {
            "query": "How can I be an effective product manager?",
            "features": [SearchOptions.PARAGRAPH, SearchOptions.VECTOR],
        },
    }
}


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/find",
    status_code=200,
    name="Find Knowledge Box",
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
    query: str = param_to_query(SearchParamDefaults.query),
    advanced_query: Optional[str] = param_to_query(SearchParamDefaults.advanced_query),
    fields: List[str] = param_to_query(SearchParamDefaults.fields),
    filters: List[str] = param_to_query(SearchParamDefaults.filters),
    faceted: List[str] = param_to_query(SearchParamDefaults.faceted),
    sort_field: SortField = param_to_query(SearchParamDefaults.sort_field),
    sort_limit: Optional[int] = param_to_query(SearchParamDefaults.sort_limit),
    sort_order: SortOrder = param_to_query(SearchParamDefaults.sort_order),
    page_number: int = param_to_query(SearchParamDefaults.page_number),
    page_size: int = param_to_query(SearchParamDefaults.page_size),
    min_score: float = param_to_query(SearchParamDefaults.min_score),
    range_creation_start: Optional[datetime] = Query(default=None),
    range_creation_end: Optional[datetime] = Query(default=None),
    range_modification_start: Optional[datetime] = Query(default=None),
    range_modification_end: Optional[datetime] = Query(default=None),
    features: List[SearchOptions] = Query(
        default=[
            SearchOptions.PARAGRAPH,
            SearchOptions.VECTOR,
        ],
    ),
    reload: bool = Query(default=True),
    debug: bool = Query(False),
    highlight: bool = param_to_query(SearchParamDefaults.highlight),
    show: List[ResourceProperties] = Query([ResourceProperties.BASIC]),
    field_type_filter: List[FieldTypeName] = Query(
        list(FieldTypeName), alias="field_type"
    ),
    extracted: List[ExtractedDataTypeName] = Query(list(ExtractedDataTypeName)),
    shards: List[str] = param_to_query(SearchParamDefaults.shards),
    with_duplicates: bool = param_to_query(SearchParamDefaults.with_duplicates),
    with_synonyms: bool = param_to_query(SearchParamDefaults.with_synonyms),
    autofilter: bool = param_to_query(SearchParamDefaults.autofilter),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxFindResults, HTTPClientError]:
    try:
        item = FindRequest(
            query=query,
            advanced_query=advanced_query,
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
            reload=reload,
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
    try:
        return await find(
            response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
        )
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/find",
    status_code=200,
    name="Find Knowledge Box",
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
    item: FindRequest = Body(examples=FIND_EXAMPLES),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxFindResults, HTTPClientError]:
    try:
        return await find(
            response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
        )
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)


async def find(
    response: Response,
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    do_audit: bool = True,
) -> KnowledgeboxFindResults:
    audit = get_audit()
    start_time = time()

    sort_options = parse_sort_options(item)

    if item.query == "" and (item.vector is None or len(item.vector) == 0):
        # If query is not defined we force to not return vector results
        if SearchOptions.VECTOR in item.features:
            item.features.remove(SearchOptions.VECTOR)

    # We need to query all nodes
    processed_query = pre_process_query(item.query)
    pb_query, incomplete_results = await global_query_to_pb(
        kbid,
        features=item.features,
        query=processed_query,
        advanced_query=item.advanced_query,
        filters=item.filters,
        faceted=item.faceted,
        sort=sort_options,
        page_number=item.page_number,
        page_size=item.page_size,
        range_creation_start=item.range_creation_start,
        range_creation_end=item.range_creation_end,
        range_modification_start=item.range_modification_start,
        range_modification_end=item.range_modification_end,
        fields=item.fields,
        reload=item.reload,
        user_vector=item.vector,
        vectorset=item.vectorset,
        with_duplicates=item.with_duplicates,
        with_status=item.with_status,
        with_synonyms=item.with_synonyms,
        autofilter=item.autofilter,
    )

    results, incomplete_results, queried_nodes, queried_shards = await node_query(
        kbid, Method.SEARCH, pb_query, item.shards
    )

    # We need to merge
    search_results = await find_merge_results(
        results,
        count=item.page_size,
        page=item.page_number,
        kbid=kbid,
        show=item.show,
        field_type_filter=item.field_type_filter,
        extracted=item.extracted,
        sort=sort_options,
        requested_relations=pb_query.relation_subgraph,
        min_score=item.min_score,
        highlight=item.highlight,
    )

    response.status_code = 206 if incomplete_results else 200

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
    return search_results
