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

import random

from nidx_protos.nodereader_pb2 import DocumentScored, ParagraphResult

from nucliadb.search.search.cut import cut_page
from nucliadb.search.search.find_merge import (
    keyword_result_to_text_block_match,
    semantic_result_to_text_block_match,
)
from nucliadb.search.search.rank_fusion import LegacyRankFusion
from nucliadb_models.search import SCORE_TYPE


def get_paragraph_result(score):
    start = random.randint(0, 10)
    end = random.randint(start, 20)
    pr = ParagraphResult()
    pr.uuid = "rid"
    pr.score.bm25 = score
    pr.score.booster = 0
    pr.paragraph = f"rid/a/title/{start}-{end}"
    pr.start = start
    pr.end = end
    pr.field = "/a/title"
    return keyword_result_to_text_block_match(pr)


def get_vector_result(score):
    start = random.randint(0, 10)
    end = random.randint(start, 20)
    index = random.randint(0, 100)
    vr = DocumentScored()
    vr.doc_id.id = f"rid/f/file/{index}/{start}-{end}"
    vr.score = score
    vr.metadata.position.start = start
    vr.metadata.position.end = end
    return semantic_result_to_text_block_match(vr)


def test_merge_paragraphs_vectors():
    shard1_paragraphs = [
        get_paragraph_result(1),
        get_paragraph_result(3),
        get_paragraph_result(4),
    ]
    shard2_paragraphs = [
        get_paragraph_result(2),
        get_paragraph_result(5),
    ]
    shard1_vectors = []
    for i in range(2):
        score = max(5 / float(i + 1), 1)
        shard1_vectors.append(get_vector_result(score))

    shard2_vectors = []
    for i in range(2, 5):
        score = max(5 / float(i + 1), 1)
        shard2_vectors.append(get_vector_result(score))

    min_score_semantic = 1
    paragraphs, next_page = cut_page(
        LegacyRankFusion(window=20).fuse(
            keyword=[*shard1_paragraphs, *shard2_paragraphs],
            semantic=filter(lambda x: x.score >= min_score_semantic, [*shard1_vectors, *shard2_vectors]),
        ),
        20,
    )
    assert not next_page
    assert len(paragraphs) == 10

    # Check that the paragraphs are ordered by score
    bm25_scores = [
        text_block.score for text_block in paragraphs if text_block.score_type == SCORE_TYPE.BM25
    ]
    vector_scores = [
        text_block.score for text_block in paragraphs if text_block.score_type == SCORE_TYPE.VECTOR
    ]
    assert bm25_scores == sorted(bm25_scores, reverse=True)
    assert vector_scores == sorted(vector_scores, reverse=True)

    vector_scores = set()
    for index, score_type in [
        (0, SCORE_TYPE.BM25),
        (1, SCORE_TYPE.VECTOR),
        (2, SCORE_TYPE.BM25),
        (3, SCORE_TYPE.BM25),
        (4, SCORE_TYPE.VECTOR),
        (5, SCORE_TYPE.BM25),
        (6, SCORE_TYPE.BM25),
        (7, SCORE_TYPE.VECTOR),
        (8, SCORE_TYPE.VECTOR),
        (9, SCORE_TYPE.VECTOR),
    ]:
        assert paragraphs[index].score_type == score_type
        if score_type == SCORE_TYPE.VECTOR:
            vector_scores.add(paragraphs[index].score)

    # Check that the vector scores are different
    assert len(vector_scores) == 5
