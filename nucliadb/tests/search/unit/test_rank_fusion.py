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

"""Rank fusion merge is used to merge results from keyword and semantic search.

This test suite validates different combinations of inputs

"""

import random
from typing import Optional, Type
from unittest.mock import patch

import pytest
from nidx_protos.nodereader_pb2 import DocumentScored, GraphSearchResponse, ParagraphResult

import nucliadb_models.search as search_models
from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId, VectorId
from nucliadb.search.search.find_merge import (
    FAKE_GRAPH_SCORE,
    graph_results_to_text_block_matches,
    keyword_result_to_text_block_match,
    semantic_result_to_text_block_match,
)
from nucliadb.search.search.query_parser.parsers import parse_find
from nucliadb.search.search.rank_fusion import ReciprocalRankFusion, get_rank_fusion
from nucliadb_models.search import SCORE_TYPE, FindRequest
from nucliadb_protos.utils_pb2 import RelationMetadata


@pytest.fixture(scope="function", autouse=True)
def disable_hidden_resources_check():
    with patch(
        "nucliadb.search.search.query_parser.parsers.find.filter_hidden_resources", return_value=False
    ):
        yield


@pytest.mark.parametrize(
    "rank_fusion,expected_type",
    [
        (search_models.RankFusionName.RECIPROCAL_RANK_FUSION, ReciprocalRankFusion),
        (search_models.ReciprocalRankFusion(), ReciprocalRankFusion),
    ],
)
async def test_get_rank_fusion(rank_fusion, expected_type: Type):
    item = FindRequest(rank_fusion=rank_fusion)
    parsed = await parse_find("kbid", item)
    assert parsed.retrieval.rank_fusion is not None
    algorithm = get_rank_fusion(parsed.retrieval.rank_fusion)
    assert isinstance(algorithm, expected_type)


def gen_keyword_result(
    score: float, rid: Optional[str] = None, force_id: Optional[str] = None
) -> TextBlockMatch:
    assert (rid is None and force_id is not None) or (rid is not None and force_id is None)
    if force_id is None:
        start = random.randint(0, 100)
        end = random.randint(start, start + 100)
        paragraph_id = ParagraphId.from_string(f"{rid}/f/my-file/{start}-{end}")
    else:
        paragraph_id = ParagraphId.from_string(force_id)

    result = ParagraphResult()
    result.uuid = paragraph_id.rid
    result.score.bm25 = score
    result.paragraph = paragraph_id.full()
    result.start = paragraph_id.paragraph_start
    result.end = paragraph_id.paragraph_end
    return keyword_result_to_text_block_match(result)


def gen_semantic_result(
    score: float, rid: Optional[str] = None, force_id: Optional[str] = None
) -> TextBlockMatch:
    assert (rid is None and force_id is not None) or (rid is not None and force_id is None)
    if force_id is None:
        start = random.randint(0, 100)
        end = random.randint(start, start + 100)
        index = random.randint(0, 100)
        vector_id = VectorId.from_string(f"{rid}/f/my-file/{index}/{start}-{end}")
    else:
        vector_id = VectorId.from_string(force_id)

    result = DocumentScored()
    result.doc_id.id = vector_id.full()
    result.score = score
    result.metadata.position.start = vector_id.vector_start
    result.metadata.position.end = vector_id.vector_end
    return semantic_result_to_text_block_match(result)


def gen_graph_result(rid: Optional[str] = None, force_id: Optional[str] = None) -> TextBlockMatch:
    assert (rid is None and force_id is not None) or (rid is not None and force_id is None)
    if force_id is None:
        start = random.randint(0, 100)
        end = random.randint(start, start + 100)
        paragraph_id = ParagraphId.from_string(f"{rid}/f/my-file/{start}-{end}")
    else:
        paragraph_id = ParagraphId.from_string(force_id)

    return graph_results_to_text_block_matches(
        GraphSearchResponse(
            graph=[GraphSearchResponse.Path(metadata=RelationMetadata(paragraph_id=paragraph_id.full()))]
        )
    )[0]


RRF_TEST_K = 2


def rrf_score(rank: int) -> float:
    score = 1 / (RRF_TEST_K + rank)
    return round(score, 6)


@pytest.mark.parametrize(
    "keyword,semantic,graph,expected",
    [
        # mix of keyword, semantic and graph results
        (
            [
                gen_keyword_result(0.1, rid="k-1"),
                gen_keyword_result(0.5, rid="k-2"),
                gen_keyword_result(0.3, rid="k-3"),
            ],
            [
                gen_semantic_result(0.2, rid="s-1"),
                gen_semantic_result(0.3, rid="s-2"),
                gen_semantic_result(0.6, rid="s-3"),
                gen_semantic_result(0.4, rid="s-4"),
            ],
            [
                gen_graph_result(rid="g-1"),
                gen_graph_result(rid="g-2"),
            ],
            [
                ("k-2", round(1 / (0 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("s-3", round(1 / (0 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("g-1", round(1 / (0 + RRF_TEST_K), 6), SCORE_TYPE.RELATION_RELEVANCE),
                ("k-3", round(1 / (1 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("s-4", round(1 / (1 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("g-2", round(1 / (1 + RRF_TEST_K), 6), SCORE_TYPE.RELATION_RELEVANCE),
                ("k-1", round(1 / (2 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("s-2", round(1 / (2 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("s-1", round(1 / (3 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
            ],
        ),
        # only keyword results (we maintain original scores)
        (
            [
                gen_keyword_result(1, rid="k-1"),
                gen_keyword_result(3, rid="k-2"),
                gen_keyword_result(4, rid="k-3"),
            ],
            [],
            [],
            [
                ("k-3", 4.0, SCORE_TYPE.BM25),
                ("k-2", 3.0, SCORE_TYPE.BM25),
                ("k-1", 1.0, SCORE_TYPE.BM25),
            ],
        ),
        # only semantic results (we maintain original scores)
        (
            [],
            [
                gen_semantic_result(0.2, rid="s-1"),
                gen_semantic_result(0.3, rid="s-2"),
                gen_semantic_result(0.6, rid="s-3"),
                gen_semantic_result(0.4, rid="s-4"),
            ],
            [],
            [
                ("s-3", 0.6, SCORE_TYPE.VECTOR),
                ("s-4", 0.4, SCORE_TYPE.VECTOR),
                ("s-2", 0.3, SCORE_TYPE.VECTOR),
                ("s-1", 0.2, SCORE_TYPE.VECTOR),
            ],
        ),
        # only graph results (we maintain original scores)
        (
            [],
            [],
            [
                gen_graph_result(rid="g-1"),
                gen_graph_result(rid="g-2"),
            ],
            [
                ("g-1", FAKE_GRAPH_SCORE, SCORE_TYPE.RELATION_RELEVANCE),
                ("g-2", FAKE_GRAPH_SCORE, SCORE_TYPE.RELATION_RELEVANCE),
            ],
        ),
        # all keyword scores greater than semantic
        (
            [
                gen_keyword_result(1, rid="k-1"),
                gen_keyword_result(5, rid="k-2"),
                gen_keyword_result(3, rid="k-3"),
            ],
            [
                gen_semantic_result(0.2, rid="s-1"),
                gen_semantic_result(0.3, rid="s-2"),
                gen_semantic_result(0.6, rid="s-3"),
                gen_semantic_result(0.4, rid="s-4"),
                gen_semantic_result(0.1, rid="s-5"),
            ],
            [],
            [
                ("k-2", round(1 / (0 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("s-3", round(1 / (0 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("k-3", round(1 / (1 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("s-4", round(1 / (1 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("k-1", round(1 / (2 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("s-2", round(1 / (2 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("s-1", round(1 / (3 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("s-5", round(1 / (4 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
            ],
        ),
        # all keyword scores smaller than semantic
        (
            [
                gen_keyword_result(0.1, rid="k-1"),
                gen_keyword_result(0.5, rid="k-2"),
                gen_keyword_result(0.3, rid="k-3"),
                gen_keyword_result(0.6, rid="k-4"),
                gen_keyword_result(0.6, rid="k-5"),
            ],
            [
                gen_semantic_result(2, rid="s-1"),
                gen_semantic_result(3, rid="s-2"),
                gen_semantic_result(6, rid="s-3"),
            ],
            [],
            [
                ("k-4", round(1 / (0 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("s-3", round(1 / (0 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("k-5", round(1 / (1 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("s-2", round(1 / (1 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("k-2", round(1 / (2 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("s-1", round(1 / (2 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("k-3", round(1 / (3 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
                ("k-1", round(1 / (4 + RRF_TEST_K), 6), SCORE_TYPE.BM25),
            ],
        ),
        # multi-match
        (
            [
                gen_keyword_result(0.1, force_id="r-1/f/my/0-10"),
                gen_keyword_result(0.5, force_id="r-2/f/my/0-10"),
                gen_keyword_result(0.3, force_id="r-4/f/my/0-10"),
            ],
            [
                gen_semantic_result(2, force_id="r-1/f/my/0/0-10"),
                gen_semantic_result(3, force_id="r-3/f/my/0/0-10"),
                gen_semantic_result(6, force_id="r-4/f/my/0/0-10"),
                gen_semantic_result(6, force_id="r-5/f/my/0/0-10"),
            ],
            [],
            [
                ("r-4", round(1 / (1 + RRF_TEST_K) + 1 / (0 + RRF_TEST_K), 6), SCORE_TYPE.BOTH),
                ("r-2", round(1 / (0 + RRF_TEST_K) + 0, 6), SCORE_TYPE.BM25),
                ("r-1", round(1 / (2 + RRF_TEST_K) + 1 / (3 + RRF_TEST_K), 6), SCORE_TYPE.BOTH),
                ("r-5", round(0 + 1 / (1 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
                ("r-3", round(0 + 1 / (2 + RRF_TEST_K), 6), SCORE_TYPE.VECTOR),
            ],
        ),
    ],
)
def test_reciprocal_rank_fusion_algorithm(
    keyword: list[TextBlockMatch],
    semantic: list[TextBlockMatch],
    graph: list[TextBlockMatch],
    expected: list[tuple[str, float, SCORE_TYPE]],
):
    rrf = ReciprocalRankFusion(k=RRF_TEST_K, window=20)
    merged = rrf.fuse(keyword, semantic, graph)
    results = [(item.paragraph_id.rid, round(item.score, 6), item.score_type) for item in merged]
    assert results == expected


@pytest.mark.parametrize(
    "keyword,semantic,graph,expected",
    [
        # only keyword results (we maintain original scores)
        (
            [
                gen_keyword_result(1, rid="k-1"),
                gen_keyword_result(3, rid="k-2"),
                gen_keyword_result(4, rid="k-3"),
            ],
            [],
            [],
            [
                ("k-3", 4.0, SCORE_TYPE.BM25),
                ("k-2", 3.0, SCORE_TYPE.BM25),
                ("k-1", 1.0, SCORE_TYPE.BM25),
            ],
        ),
        # only semantic results
        (
            [],
            [
                gen_semantic_result(0.2, rid="s-1"),
                gen_semantic_result(0.3, rid="s-2"),
                gen_semantic_result(0.6, rid="s-3"),
            ],
            [],
            [
                ("s-3", 0.6, SCORE_TYPE.VECTOR),
                ("s-2", 0.3, SCORE_TYPE.VECTOR),
                ("s-1", 0.2, SCORE_TYPE.VECTOR),
            ],
        ),
        # multi-match
        (
            [
                gen_keyword_result(0.1, force_id="r-1/f/my/0-10"),
                gen_keyword_result(0.5, force_id="r-2/f/my/0-10"),
                gen_keyword_result(0.3, force_id="r-4/f/my/0-10"),
            ],
            [
                gen_semantic_result(2, force_id="r-1/f/my/0/0-10"),
                gen_semantic_result(3, force_id="r-3/f/my/0/0-10"),
                gen_semantic_result(6, force_id="r-4/f/my/0/0-10"),
                gen_semantic_result(6, force_id="r-5/f/my/0/0-10"),
            ],
            [
                gen_graph_result(force_id="r-1/f/my/0-10"),
                gen_graph_result(force_id="r-6/f/my/0-10"),
            ],
            [
                (
                    "r-1",
                    round(
                        (1 / (2 + RRF_TEST_K) * 2)
                        + (1 / (3 + RRF_TEST_K) * 0.5 + (1 / (0 + RRF_TEST_K) * FAKE_GRAPH_SCORE)),
                        6,
                    ),
                    SCORE_TYPE.BOTH,
                ),
                ("r-2", round((1 / (0 + RRF_TEST_K) * 2) + 0, 6), SCORE_TYPE.BM25),
                (
                    "r-4",
                    round((1 / (1 + RRF_TEST_K) * 2) + (1 / (0 + RRF_TEST_K) * 0.5), 6),
                    SCORE_TYPE.BOTH,
                ),
                (
                    "r-6",
                    round(0 + (1 / (1 + RRF_TEST_K) * FAKE_GRAPH_SCORE), 6),
                    SCORE_TYPE.RELATION_RELEVANCE,
                ),
                ("r-5", round(0 + (1 / (1 + RRF_TEST_K) * 0.5), 6), SCORE_TYPE.VECTOR),
                ("r-3", round(0 + (1 / (2 + RRF_TEST_K) * 0.5), 6), SCORE_TYPE.VECTOR),
            ],
        ),
    ],
)
def test_reciprocal_rank_fusion_boosting(
    keyword: list[TextBlockMatch],
    semantic: list[TextBlockMatch],
    graph: list[TextBlockMatch],
    expected: list[tuple[str, float]],
):
    rrf = ReciprocalRankFusion(k=RRF_TEST_K, window=20, keyword_weight=2, semantic_weight=0.5)
    merged = rrf.fuse(keyword, semantic, graph)
    results = [(item.paragraph_id.rid, round(item.score, 6), item.score_type) for item in merged]
    assert results == expected
