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
from nucliadb_protos.nodereader_pb2 import (
    ParagraphSearchRequest,
    ParagraphSearchResponse,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)
from nucliadb_protos.noderesources_pb2 import Shard, ShardId

from nucliadb.ingest.orm.node import Node


def query_shard(node: Node, shard: str, query: SearchRequest) -> SearchResponse:
    query.shard = shard
    return node.reader.Search(query)


def get_shard(node: Node, shard: str) -> Shard:
    shardid = ShardId()
    shardid.id = shard
    return node.reader.GetShard(shardid)


def query_paragraph_shard(
    node: Node, shard: str, query: ParagraphSearchRequest
) -> ParagraphSearchResponse:
    query.id = shard
    return node.reader.ParagraphSearch(query)


def suggest_shard(node: Node, shard: str, query: SuggestRequest) -> SuggestResponse:
    query.shard = shard
    return node.reader.Suggest(query)
