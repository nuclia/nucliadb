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
from typing import Optional

from nucliadb_protos.nodereader_pb2 import (
    GetShardRequest,
    ParagraphSearchRequest,
    ParagraphSearchResponse,
    RelationSearchRequest,
    RelationSearchResponse,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)
from nucliadb_protos.noderesources_pb2 import Shard

from nucliadb.common.cluster.abc import AbstractIndexNode
from nucliadb_telemetry import metrics

node_observer = metrics.Observer("node_client", labels={"type": ""})


async def query_shard(
    node: AbstractIndexNode, shard: str, query: SearchRequest
) -> SearchResponse:
    req = SearchRequest()
    req.CopyFrom(query)
    req.shard = shard
    with node_observer({"type": "search"}):
        return await node.reader.Search(req)  # type: ignore


async def get_shard(
    node: AbstractIndexNode, shard_id: str, vectorset: Optional[str] = None
) -> Shard:
    req = GetShardRequest()
    req.shard_id.id = shard_id
    if vectorset is not None:
        req.vectorset = vectorset
    with node_observer({"type": "get_shard"}):
        return await node.reader.GetShard(req)  # type: ignore


async def query_paragraph_shard(
    node: AbstractIndexNode, shard: str, query: ParagraphSearchRequest
) -> ParagraphSearchResponse:
    req = ParagraphSearchRequest()
    req.CopyFrom(query)
    req.id = shard
    with node_observer({"type": "paragraph_search"}):
        return await node.reader.ParagraphSearch(req)  # type: ignore


async def suggest_shard(
    node: AbstractIndexNode, shard: str, query: SuggestRequest
) -> SuggestResponse:
    req = SuggestRequest()
    req.CopyFrom(query)
    req.shard = shard
    with node_observer({"type": "suggest"}):
        return await node.reader.Suggest(req)  # type: ignore


async def relations_shard(
    node: AbstractIndexNode, shard: str, query: RelationSearchRequest
) -> RelationSearchResponse:
    req = RelationSearchRequest()
    req.CopyFrom(query)
    req.shard_id = shard
    with node_observer({"type": "relation_search"}):
        return await node.reader.RelationSearch(req)  # type: ignore
