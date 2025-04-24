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

import backoff
from grpc import StatusCode
from grpc.aio import AioRpcError
from nidx_protos.nodereader_pb2 import (
    GetShardRequest,
    GraphSearchRequest,
    GraphSearchResponse,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)
from nidx_protos.noderesources_pb2 import Shard

from nucliadb.common.nidx import get_nidx_api_client, get_nidx_searcher_client


def should_giveup(e: Exception):
    if isinstance(e, AioRpcError) and e.code() != StatusCode.NOT_FOUND:
        return True
    return False


@backoff.on_exception(
    backoff.expo, Exception, jitter=None, factor=0.1, max_tries=3, giveup=should_giveup
)
async def query_shard(shard: str, query: SearchRequest) -> SearchResponse:
    req = SearchRequest()
    req.CopyFrom(query)
    req.shard = shard
    return await get_nidx_searcher_client().Search(req)


@backoff.on_exception(
    backoff.expo, Exception, jitter=None, factor=0.1, max_tries=3, giveup=should_giveup
)
async def get_shard(shard_id: str) -> Shard:
    req = GetShardRequest()
    req.shard_id.id = shard_id
    return await get_nidx_api_client().GetShard(req)


@backoff.on_exception(
    backoff.expo, Exception, jitter=None, factor=0.1, max_tries=3, giveup=should_giveup
)
async def suggest_shard(shard: str, query: SuggestRequest) -> SuggestResponse:
    req = SuggestRequest()
    req.CopyFrom(query)
    req.shard = shard
    return await get_nidx_searcher_client().Suggest(req)


@backoff.on_exception(
    backoff.expo, Exception, jitter=None, factor=0.1, max_tries=3, giveup=should_giveup
)
async def graph_search_shard(shard: str, query: GraphSearchRequest) -> GraphSearchResponse:
    req = GraphSearchRequest()
    req.CopyFrom(query)
    req.shard = shard
    return await get_nidx_searcher_client().GraphSearch(req)
