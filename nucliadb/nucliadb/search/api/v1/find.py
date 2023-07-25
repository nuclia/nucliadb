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
from typing import List, Optional, Tuple, Union

from fastapi import Body, Header, Request, Response
from fastapi_versioning import version
from pydantic.error_wrappers import ValidationError

from nucliadb.ingest.orm.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.find_merge import find_merge_results
from nucliadb.search.search.query import (
    get_default_min_score,
    global_query_to_pb,
    pre_process_query,
)
from nucliadb.search.search.utils import should_disable_vector_search
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
    query: str = fastapi_query(SearchParamDefaults.query),
    fields: List[str] = fastapi_query(SearchParamDefaults.fields),
    filters: List[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: List[str] = fastapi_query(SearchParamDefaults.faceted),
    page_number: int = fastapi_query(SearchParamDefaults.page_number),
    page_size: int = fastapi_query(SearchParamDefaults.page_size),
    min_score: float = fastapi_query(SearchParamDefaults.min_score),
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
    features: List[SearchOptions] = fastapi_query(
        SearchParamDefaults.search_features,
        default=[
            SearchOptions.PARAGRAPH,
            SearchOptions.VECTOR,
        ],
    ),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
    highlight: bool = fastapi_query(SearchParamDefaults.highlight),
    show: List[ResourceProperties] = fastapi_query(SearchParamDefaults.show),
    field_type_filter: List[FieldTypeName] = fastapi_query(
        SearchParamDefaults.field_type_filter, alias="field_type"
    ),
    extracted: List[ExtractedDataTypeName] = fastapi_query(
        SearchParamDefaults.extracted
    ),
    shards: List[str] = fastapi_query(SearchParamDefaults.shards),
    with_duplicates: bool = fastapi_query(SearchParamDefaults.with_duplicates),
    with_synonyms: bool = fastapi_query(SearchParamDefaults.with_synonyms),
    autofilter: bool = fastapi_query(SearchParamDefaults.autofilter),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[KnowledgeboxFindResults, HTTPClientError]:
    try:
        item = FindRequest(
            query=query,
            fields=fields,
            filters=filters,
            faceted=faceted,
            page_number=page_number,
            page_size=page_size,
            min_score=min_score,
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
        )
    except ValidationError as exc:
        detail = json.loads(exc.json())
        return HTTPClientError(status_code=422, detail=detail)
    try:
        results, _ = await find(
            response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
        )
        return results
    except KnowledgeBoxNotFound:
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")
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
        results, incomplete = await find(
            response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
        )
        response.status_code = 206 if incomplete else 200
        return results
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
) -> Tuple[KnowledgeboxFindResults, bool]:
    audit = get_audit()
    start_time = time()

    if SearchOptions.VECTOR in item.features:
        if should_disable_vector_search(item):
            item.features.remove(SearchOptions.VECTOR)

    min_score = item.min_score
    if min_score is None:
        min_score = await get_default_min_score(kbid)

    # We need to query all nodes
    processed_query = pre_process_query(item.query)
    pb_query, incomplete_results, autofilters = await global_query_to_pb(
        kbid,
        features=item.features,
        query=processed_query,
        filters=item.filters,
        faceted=item.faceted,
        sort=None,
        page_number=item.page_number,
        page_size=item.page_size,
        min_score=min_score,
        range_creation_start=item.range_creation_start,
        range_creation_end=item.range_creation_end,
        range_modification_start=item.range_modification_start,
        range_modification_end=item.range_modification_end,
        fields=item.fields,
        user_vector=item.vector,
        vectorset=item.vectorset,
        with_duplicates=item.with_duplicates,
        with_synonyms=item.with_synonyms,
        autofilter=item.autofilter,
        key_filters=item.resource_filters,
    )
    results, query_incomplete_results, queried_nodes, queried_shards = await node_query(
        kbid, Method.SEARCH, pb_query, item.shards
    )

    incomplete_results = incomplete_results or query_incomplete_results

    # We need to merge
    search_results = await find_merge_results(
        results,
        count=item.page_size,
        page=item.page_number,
        kbid=kbid,
        show=item.show,
        field_type_filter=item.field_type_filter,
        extracted=item.extracted,
        requested_relations=pb_query.relation_subgraph,
        min_score=min_score,
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
        search_results.nodes = queried_nodes

    search_results.shards = queried_shards
    search_results.autofilters = autofilters
    return search_results, incomplete_results
