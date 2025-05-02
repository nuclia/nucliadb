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
from typing import Union

from nidx_protos.nodereader_pb2 import SearchRequest, SearchResponse

from nucliadb.common.external_index_providers.pinecone import PineconeIndexManager
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.find_merge import (
    graph_results_to_text_block_matches,
    keyword_results_to_text_block_matches,
    merge_shard_responses,
    semantic_results_to_text_block_matches,
)

from .models import ExecutionContext, IndexResult, PlanStep


class NidxQuery(PlanStep):
    """Perform a nidx search and return results as text block matches"""

    def __init__(self, kbid: str, pb_query: SearchRequest):
        self.kbid = kbid
        self.pb_query = pb_query

    async def execute(self, context: ExecutionContext) -> SearchResponse:
        index_results, incomplete, queried_shards = await node_query(
            self.kbid, Method.SEARCH, self.pb_query
        )
        context.nidx_incomplete = incomplete
        context.nidx_queried_shards = queried_shards

        search_response = merge_shard_responses(index_results)

        context.next_page = context.next_page or search_response.paragraph.next_page
        context.keyword_result_count = context.next_page or search_response.paragraph.next_page
        context.keyword_exact_matches = list(search_response.paragraph.ematches)

        return search_response

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {"NidxQuery": "..."}


class ProtoIntoTextBlockMatches(PlanStep):
    def __init__(self, uses: PlanStep[SearchResponse]):
        self.proto_query = uses

    async def execute(self, context: ExecutionContext) -> IndexResult:
        search_response = await self.proto_query.execute(context)

        keyword = keyword_results_to_text_block_matches(search_response.paragraph.results)
        semantic = semantic_results_to_text_block_matches(search_response.vector.documents)
        graph = graph_results_to_text_block_matches(search_response.graph)

        return IndexResult(keyword=keyword, semantic=semantic, graph=graph)

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "ProtoIntoTextBlockMatches": self.proto_query.explain(),
        }


class PineconeQuery(PlanStep):
    def __init__(self, kbid: str, pb_query: SearchRequest, pinecone_index: PineconeIndexManager):
        self.kbid = kbid
        self.pb_query = pb_query
        self.pinecone_index = pinecone_index

    async def execute(self, _context: ExecutionContext) -> IndexResult:
        query_results = await self.pinecone_index.query(self.pb_query)
        return IndexResult(
            semantic=list(query_results.iter_matching_text_blocks()),
            # keyword and graph are not supported in Pinecone.
            keyword=[],
            graph=[],
        )

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return "PineconeQuery"


class IndexPostFilter(PlanStep):
    def __init__(self, index_query: PlanStep[IndexResult], semantic_min_score: float):
        self.index_query = index_query
        self.semantic_min_score = semantic_min_score

    async def execute(self, context: ExecutionContext) -> IndexResult:
        results = await self.index_query.execute(context)
        results.semantic = list(filter(lambda x: x.score >= self.semantic_min_score, results.semantic))
        return results

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "IndexPostFilter": self.index_query.explain(),
        }
