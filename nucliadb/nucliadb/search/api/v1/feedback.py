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
import asyncio
from datetime import datetime
from time import time
from typing import List, Optional

from fastapi import Body, Header, HTTPException, Query, Request, Response
from fastapi_versioning import version
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.nodereader_pb2 import SearchResponse
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject
from sentry_sdk import capture_exception

from nucliadb.search import logger
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search.fetch import abort_transaction  # type: ignore
from nucliadb.search.search.merge import merge_results
from nucliadb.search.search.query import global_query_to_pb, pre_process_query
from nucliadb.search.search.shards import query_shard
from nucliadb.search.settings import settings
from nucliadb.search.utilities import get_nodes, get_predict
from nucliadb_models.common import FieldTypeName
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.resource import ExtractedDataTypeName, NucliaDBRoles
from nucliadb_models.search import (
    FeedbackRequest,
    KnowledgeboxSearchResults,
    NucliaDBClientType,
    ResourceProperties,
    SearchOptions,
    SearchRequest,
    SortField,
    SortFieldMap,
    SortOptions,
    SortOrder,
    SortOrderMap,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import ShardsNotFound
from nucliadb_utils.utilities import get_audit


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/feedback",
    status_code=200,
    name="Feedback Knowledge Box",
    description="Feedback on a Knowledge Box",
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def feedback_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    item: FeedbackRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
):
    predict = get_predict()
    predict
    return await search(
        response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
    )
