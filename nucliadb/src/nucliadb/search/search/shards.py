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

from nidx_protos.nodereader_pb2 import (
    GraphSearchRequest,
    GraphSearchResponse,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)
from nidx_protos.noderesources_pb2 import Shard


async def query_shards(shards: list[str], query: SearchRequest) -> SearchResponse:
    # TODO(Marklogic): Execute the search request against the indexing backend.
    return SearchResponse()


async def get_shard(shard_id: str) -> Shard:
    # TODO(Marklogic): Read shard statistics from the indexing backend.
    return Shard()


async def suggest_shards(shards: list[str], query: SuggestRequest) -> SuggestResponse:
    # TODO(Marklogic): Execute the suggestion request against the indexing backend.
    return SuggestResponse()


async def graph_search_shards(shards: list[str], query: GraphSearchRequest) -> GraphSearchResponse:
    # TODO(Marklogic): Execute the graph request against the indexing backend.
    return GraphSearchResponse()
