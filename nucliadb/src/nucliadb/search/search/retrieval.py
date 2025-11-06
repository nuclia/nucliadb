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


from nidx_protos.nodereader_pb2 import SearchRequest, SearchResponse

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb.search.search.find_merge import (
    graph_results_to_text_block_matches,
    keyword_results_to_text_block_matches,
    merge_shard_responses,
    semantic_results_to_text_block_matches,
)
from nucliadb.search.search.query_parser.models import UnitRetrieval
from nucliadb.search.search.query_parser.parsers.unit_retrieval import convert_retrieval_to_proto
from nucliadb.search.search.rank_fusion import IndexSource, get_rank_fusion


async def nidx_search(kbid: str, pb_query: SearchRequest) -> tuple[SearchResponse, list[str]]:
    """Wrapper around nidx_query for SEARCH that merges shards results in a
    single response.

    At some point, nidx will provide this functionality and we'll be able to
    remove this.

    """
    shards_responses, queried_shards = await nidx_query(kbid, Method.SEARCH, pb_query)
    response = merge_shard_responses(shards_responses)
    return response, queried_shards


async def text_block_search(
    kbid: str, retrieval: UnitRetrieval
) -> tuple[list[TextBlockMatch], SearchRequest, SearchResponse, list[str]]:
    """Search for text blocks in multiple indexes and return an rank fused view.

    This search method provides a textual view of the data. For example, given a
    graph query, it will return the text blocks associated with matched
    triplets, not the triplet itself.

    """
    assert retrieval.rank_fusion is not None, "text block search requries a rank fusion algorithm"

    pb_query = convert_retrieval_to_proto(retrieval)
    shards_response, queried_shards = await nidx_search(kbid, pb_query)

    keyword_results = keyword_results_to_text_block_matches(shards_response.paragraph.results)
    semantic_results = semantic_results_to_text_block_matches(shards_response.vector.documents)
    graph_results = graph_results_to_text_block_matches(shards_response.graph)

    rank_fusion = get_rank_fusion(retrieval.rank_fusion)
    merged_text_blocks = rank_fusion.fuse(
        {
            IndexSource.KEYWORD: keyword_results,
            IndexSource.SEMANTIC: semantic_results,
            IndexSource.GRAPH: graph_results,
        }
    )

    # cut to the rank fusion window. As we ask each shard and index this window,
    # we'll normally have extra results
    text_blocks = merged_text_blocks[: retrieval.rank_fusion.window]

    return text_blocks, pb_query, shards_response, queried_shards
