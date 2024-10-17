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

import pytest

from nucliadb.common.ids import ParagraphId
from nucliadb.search.search.find_merge import rank_fusion_merge
from nucliadb_models.search import SCORE_TYPE
from nucliadb_protos.nodereader_pb2 import DocumentScored, ParagraphResult


def gen_keyword_result(rid: str, score: float) -> ParagraphResult:
    start = random.randint(0, 100)
    end = random.randint(start, start + 100)
    paragraph_id = ParagraphId.from_string(f"{rid}/f/my-file/{start}-{end}")

    result = ParagraphResult()
    result.uuid = rid
    result.score.bm25 = score
    result.paragraph = paragraph_id.full()
    result.start = start
    result.end = end
    return result


def gen_semantic_result(rid: str, score: float) -> DocumentScored:
    start = random.randint(0, 100)
    end = random.randint(start, start + 100)
    index = random.randint(0, 100)

    result = DocumentScored()
    result.doc_id.id = f"{rid}/f/my-file/{index}/{start}-{end}"
    result.score = score
    result.metadata.position.start = start
    result.metadata.position.end = end
    return result


@pytest.mark.parametrize(
    "keyword,semantic,expected",
    [
        # mix of keyword and semantic results
        (
            [
                gen_keyword_result("k-1", 0.1),
                gen_keyword_result("k-2", 0.5),
                gen_keyword_result("k-3", 0.3),
            ],
            [
                gen_semantic_result("s-1", 0.2),
                gen_semantic_result("s-2", 0.3),
                gen_semantic_result("s-3", 0.6),
                gen_semantic_result("s-4", 0.4),
            ],
            [
                ("k-2", 0.5, SCORE_TYPE.BM25),
                ("s-3", 0.6, SCORE_TYPE.VECTOR),
                ("k-3", 0.3, SCORE_TYPE.BM25),
                ("k-1", 0.1, SCORE_TYPE.BM25),
                ("s-4", 0.4, SCORE_TYPE.VECTOR),
                ("s-2", 0.3, SCORE_TYPE.VECTOR),
                ("s-1", 0.2, SCORE_TYPE.VECTOR),
            ],
        ),
        # only keyword results
        (
            [
                gen_keyword_result("k-1", 1),
                gen_keyword_result("k-2", 3),
                gen_keyword_result("k-3", 4),
            ],
            [],
            [
                ("k-3", 4, SCORE_TYPE.BM25),
                ("k-2", 3, SCORE_TYPE.BM25),
                ("k-1", 1, SCORE_TYPE.BM25),
            ],
        ),
        # only semantic results
        (
            [],
            [
                gen_semantic_result("s-1", 0.2),
                gen_semantic_result("s-2", 0.3),
                gen_semantic_result("s-3", 0.6),
                gen_semantic_result("s-4", 0.4),
            ],
            [
                ("s-3", 0.6, SCORE_TYPE.VECTOR),
                ("s-4", 0.4, SCORE_TYPE.VECTOR),
                ("s-2", 0.3, SCORE_TYPE.VECTOR),
                ("s-1", 0.2, SCORE_TYPE.VECTOR),
            ],
        ),
        # all keyword scores greater than semantic
        (
            [
                gen_keyword_result("k-1", 1),
                gen_keyword_result("k-2", 5),
                gen_keyword_result("k-3", 3),
            ],
            [
                gen_semantic_result("s-1", 0.2),
                gen_semantic_result("s-2", 0.3),
                gen_semantic_result("s-3", 0.6),
                gen_semantic_result("s-4", 0.4),
                gen_semantic_result("s-5", 0.1),
            ],
            [
                ("k-2", 5, SCORE_TYPE.BM25),
                ("s-3", 0.6, SCORE_TYPE.VECTOR),
                ("k-3", 3, SCORE_TYPE.BM25),
                ("k-1", 1, SCORE_TYPE.BM25),
                ("s-4", 0.4, SCORE_TYPE.VECTOR),
                ("s-2", 0.3, SCORE_TYPE.VECTOR),
                ("s-1", 0.2, SCORE_TYPE.VECTOR),
                ("s-5", 0.1, SCORE_TYPE.VECTOR),
            ],
        ),
        # all keyword scores smaller than semantic
        (
            [
                gen_keyword_result("k-1", 0.1),
                gen_keyword_result("k-2", 0.5),
                gen_keyword_result("k-3", 0.3),
                gen_keyword_result("k-4", 0.6),
                gen_keyword_result("k-5", 0.6),
            ],
            [
                gen_semantic_result("s-1", 2),
                gen_semantic_result("s-2", 3),
                gen_semantic_result("s-3", 6),
            ],
            [
                ("k-4", 0.6, SCORE_TYPE.BM25),
                ("s-3", 6.0, SCORE_TYPE.VECTOR),
                ("k-5", 0.6, SCORE_TYPE.BM25),
                ("k-2", 0.5, SCORE_TYPE.BM25),
                ("s-2", 3.0, SCORE_TYPE.VECTOR),
                ("k-3", 0.3, SCORE_TYPE.BM25),
                ("k-1", 0.1, SCORE_TYPE.BM25),
                ("s-1", 2.0, SCORE_TYPE.VECTOR),
            ],
        ),
    ],
)
def test_rank_fusion_algorithm(
    keyword: list[ParagraphResult],
    semantic: list[DocumentScored],
    expected: list[tuple[str, float, SCORE_TYPE]],
):
    """Basic test to validate how our rank fusion algorithm works"""
    merged = rank_fusion_merge(keyword, semantic)
    results = [(item.paragraph_id.rid, round(item.score, 1), item.score_type) for item in merged]
    assert results == expected
