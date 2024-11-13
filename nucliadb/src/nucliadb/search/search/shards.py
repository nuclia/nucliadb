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

from nucliadb.common.cluster.base import AbstractIndexNode
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
from nucliadb_telemetry import metrics

node_observer = metrics.Observer(
    "node_client",
    labels={"type": "", "node_id": ""},
    error_mappings={
        "timeout": asyncio.CancelledError,
    },
)


async def query_shard(node: AbstractIndexNode, shard: str, query: SearchRequest) -> SearchResponse:
    req = SearchRequest()
    req.CopyFrom(query)
    req.shard = shard
    with node_observer({"type": "search", "node_id": node.id}):
        return await node.reader.Search(req)  # type: ignore


async def get_shard(node: AbstractIndexNode, shard_id: str) -> Shard:
    req = GetShardRequest()
    req.shard_id.id = shard_id
    with node_observer({"type": "get_shard", "node_id": node.id}):
        return await node.reader.GetShard(req)  # type: ignore


async def query_paragraph_shard(
    node: AbstractIndexNode, shard: str, query: ParagraphSearchRequest
) -> ParagraphSearchResponse:
    req = SearchRequest()
    req.shard = shard
    req.paragraph = True

    req.body = query.body
    req.filter.CopyFrom(query.filter)
    req.fields.extend(query.fields)
    req.key_filters.extend(query.key_filters)
    req.order.CopyFrom(query.order)
    req.faceted.CopyFrom(query.faceted)
    req.page_number = query.page_number
    req.result_per_page = query.result_per_page
    req.timestamps.CopyFrom(query.timestamps)
    req.with_duplicates = query.with_duplicates
    req.only_faceted = query.only_faceted
    req.min_score_bm25 = query.min_score
    req.reload = query.reload  # DEPRECATED

    # optional fields must be checked, otherwise we'll set it with default value
    # instead of leave it unset. In Rust-land, we'll be changing a None for a
    # Some("")
    if query.HasField("advanced_query"):
        req.advanced_query = query.advanced_query
    if query.HasField("security"):
        req.security.CopyFrom(query.security)

    # As search request don't have a rid field to filter, we pass it as a
    # key_filter, that internally will look for "rid" or "rid/field" like
    # strings and apply the proper filter
    if query.uuid:
        req.key_filters.append(query.uuid)

    with node_observer({"type": "paragraph_search", "node_id": node.id}):
        response = await node.reader.Search(req)  # type: ignore
        return response.paragraph


async def suggest_shard(node: AbstractIndexNode, shard: str, query: SuggestRequest) -> SuggestResponse:
    req = SuggestRequest()
    req.CopyFrom(query)
    req.shard = shard
    with node_observer({"type": "suggest", "node_id": node.id}):
        return await node.reader.Suggest(req)  # type: ignore


async def relations_shard(
    node: AbstractIndexNode, shard: str, query: RelationSearchRequest
) -> RelationSearchResponse:
    req = SearchRequest(
        shard=shard,
        relation_prefix=query.prefix,
        relation_subgraph=query.subgraph,
        reload=query.reload,
    )
    with node_observer({"type": "relation_search", "node_id": node.id}):
        response = await node.reader.Search(req)  # type: ignore
    return response.relation
