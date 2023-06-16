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
from binascii import Incomplete
import json
from datetime import datetime
from time import time
from typing import List, Optional, Tuple, Union

from fastapi import Body, Header, Query, Request, Response
from fastapi_versioning import version
from pydantic.error_wrappers import ValidationError

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.find_merge import find_merge_results
from nucliadb.search.search.query import global_query_to_pb, pre_process_query
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import (
    FindRequest,
    KnowledgeboxFindResults,
    ResourceFindResults,
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


class ResourceNotFoundError(Exception):
    pass


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/resource/{{rid}}/find",
    status_code=200,
    name="Find on a Resource",
    description="Find on a Resource",
    response_model=ResourceFindResults,
    response_model_exclude_unset=True,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def find_on_resource_by_id(
    request: Request,
    response: Response,
    kbid: str,
    rid: str,
    item: FindRequest = Body(examples=FIND_EXAMPLES),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[ResourceFindResults, HTTPClientError]:
    return await find_on_resource_endpoint(
        response,
        kbid,
        rid=rid,
        rslug=None,
        item=item,
        x_ndb_client=x_ndb_client,
        x_nucliadb_user=x_nucliadb_user,
        x_forwarded_for=x_forwarded_for,
    )


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/resource/s/{{rslug}}/find",
    status_code=200,
    name="Find on a Resource",
    description="Find on a Resource",
    response_model=ResourceFindResults,
    response_model_exclude_unset=True,
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def find_on_resource_by_slug(
    request: Request,
    response: Response,
    kbid: str,
    rslug: str,
    item: FindRequest = Body(examples=FIND_EXAMPLES),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[ResourceFindResults, HTTPClientError]:
    return await find_on_resource_endpoint(
        response,
        kbid,
        rid=None,
        rslug=rslug,
        item=item,
        x_ndb_client=x_ndb_client,
        x_nucliadb_user=x_nucliadb_user,
        x_forwarded_for=x_forwarded_for,
    )


async def find_on_resource_endpoint(
    response: Response,
    kbid: str,
    rid: Optional[str],
    rslug: Optional[str],
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
) -> Union[ResourceFindResults, HTTPClientError]:
    # All endpoint / view logic should be in this function
    try:
        results, incomplete = await find_on_resource(
            kbid, rid, rslug, item, x_ndb_client, x_nucliadb_user, x_forwarded_for,
        )
        response.status_code = 206 if incomplete else 200   
        return results
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)


async def find_on_resource(
    kbid: str,
    rid: Optional[str],
    rslug: Optional[str],
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    do_audit: bool = False,
) -> Tuple[KnowledgeboxFindResults, bool]:
    start_time = time()

    if all([rid, rslug]) or not any([rid, rslug]):
        raise ValueError("You must provide either rid or rslug")

    if rid is None:
        # We need the resource uuid to filter the search results
        rid = await get_resource_uuid_by_slug(kbid, rslug)  # type: ignore
        if rid is None:
            raise ResourceNotFoundError()

    if item.query == "" and (item.vector is None or len(item.vector) == 0):
        # If query is not defined we force to not return vector results
        if SearchOptions.VECTOR in item.features:
            item.features.remove(SearchOptions.VECTOR)

    # We need to query all nodes
    processed_query = pre_process_query(item.query)
    pb_query, incomplete_results, autofilters = await global_query_to_pb(
        kbid,
        features=item.features,
        query=processed_query,
        advanced_query=item.advanced_query,
        filters=item.filters,
        faceted=item.faceted,
        sort=None,
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
        with_synonyms=item.with_synonyms,
        autofilter=item.autofilter,
        filter_keys=[rid],
    )
    results, incomplete_results, queried_nodes, queried_shards = await node_query(
        kbid, Method.SEARCH, pb_query, item.shards
    )

    # We need to merge
    search_results = await find_resource_merge_results(
        results,
        count=item.page_size,
        page=item.page_number,
        kbid=kbid,
        show=item.show,
        field_type_filter=item.field_type_filter,
        extracted=item.extracted,
        requested_relations=pb_query.relation_subgraph,
        min_score=item.min_score,
        highlight=item.highlight,
    )

    audit = get_audit()
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
