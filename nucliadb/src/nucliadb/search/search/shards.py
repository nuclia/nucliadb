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

import backoff
from grpc import StatusCode
from grpc.aio import AioRpcError

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb_protos.nodereader_pb2 import (
    GetShardRequest,
    GraphSearchRequest,
    GraphSearchResponse,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)
from nucliadb_protos.noderesources_pb2 import Shard
from nucliadb_telemetry import metrics

node_observer = metrics.Observer(
    "node_client",
    labels={"type": "", "node_id": ""},
    error_mappings={
        "timeout": asyncio.CancelledError,
    },
)


def should_giveup(e: Exception):
    if isinstance(e, AioRpcError) and e.code() != StatusCode.NOT_FOUND:
        return True
    return False


@backoff.on_exception(
    backoff.expo, Exception, jitter=None, factor=0.1, max_tries=3, giveup=should_giveup
)
async def query_shard(node: AbstractIndexNode, shard: str, query: SearchRequest) -> SearchResponse:
    req = SearchRequest()
    req.CopyFrom(query)
    req.shard = shard
    with node_observer({"type": "search", "node_id": node.id}):
        return await node.reader.Search(req)  # type: ignore


@backoff.on_exception(
    backoff.expo, Exception, jitter=None, factor=0.1, max_tries=3, giveup=should_giveup
)
async def get_shard(node: AbstractIndexNode, shard_id: str) -> Shard:
    req = GetShardRequest()
    req.shard_id.id = shard_id
    with node_observer({"type": "get_shard", "node_id": node.id}):
        return await node.reader.GetShard(req)  # type: ignore


@backoff.on_exception(
    backoff.expo, Exception, jitter=None, factor=0.1, max_tries=3, giveup=should_giveup
)
async def suggest_shard(node: AbstractIndexNode, shard: str, query: SuggestRequest) -> SuggestResponse:
    req = SuggestRequest()
    req.CopyFrom(query)
    req.shard = shard
    with node_observer({"type": "suggest", "node_id": node.id}):
        return await node.reader.Suggest(req)  # type: ignore


@backoff.on_exception(
    backoff.expo, Exception, jitter=None, factor=0.1, max_tries=3, giveup=should_giveup
)
async def graph_search_shard(
    node: AbstractIndexNode, shard: str, query: GraphSearchRequest
) -> GraphSearchResponse:
    req = GraphSearchRequest()
    req.CopyFrom(query)
    req.shard = shard
    with node_observer({"type": "graph_search", "node_id": node.id}):
        return await node.reader.GraphSearch(req)  # type: ignore
