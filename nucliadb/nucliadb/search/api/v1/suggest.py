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
from time import time
from typing import List, Optional

from fastapi import Header, Request, Response
from fastapi_versioning import version

from nucliadb.ingest.txn_utils import abort_transaction
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.api.v1.utils import fastapi_query
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.merge import merge_suggest_results
from nucliadb.search.search.query import suggest_query_to_pb
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    KnowledgeboxSuggestResults,
    NucliaDBClientType,
    ResourceProperties,
    SearchParamDefaults,
    SuggestOptions,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_audit


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/suggest",
    status_code=200,
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
    fields: List[str] = fastapi_query(SearchParamDefaults.fields),
    filters: List[str] = fastapi_query(SearchParamDefaults.filters),
    faceted: List[str] = fastapi_query(SearchParamDefaults.faceted),
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
    features: List[SuggestOptions] = fastapi_query(
        SearchParamDefaults.suggest_features
    ),
    show: List[ResourceProperties] = fastapi_query(SearchParamDefaults.show),
    field_type_filter: List[FieldTypeName] = fastapi_query(
        SearchParamDefaults.field_type_filter, alias="field_type"
    ),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
    debug: bool = fastapi_query(SearchParamDefaults.debug),
    highlight: bool = fastapi_query(SearchParamDefaults.highlight),
) -> KnowledgeboxSuggestResults:
    # We need the nodes/shards that are connected to the KB
    audit = get_audit()
    start_time = time()

    # We need to query all nodes
    pb_query = suggest_query_to_pb(
        features,
        query,
        fields,
        filters,
        faceted,
        range_creation_start,
        range_creation_end,
        range_modification_start,
        range_modification_end,
    )
    results, incomplete_results, _, queried_shards = await node_query(
        kbid, Method.SUGGEST, pb_query, []
    )

    # We need to merge

    search_results = await merge_suggest_results(
        results,
        kbid=kbid,
        show=show,
        field_type_filter=field_type_filter,
        highlight=highlight,
    )
    await abort_transaction()

    response.status_code = 206 if incomplete_results else 200
    if debug and queried_shards:
        search_results.shards = queried_shards

    if audit is not None:
        await audit.suggest(
            kbid,
            x_nucliadb_user,
            x_ndb_client.to_proto(),
            x_forwarded_for,
            time() - start_time,
        )

    return search_results
