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

from nucliadb_protos.nodereader_pb2 import DocumentScored, ParagraphResult

from nucliadb.search.search.find_merge import Orderer, merge_paragraphs_vectors
from nucliadb_models.search import SCORE_TYPE


def test_orderer():
    orderer = Orderer()

    items = {}
    for i in range(30):
        key = str(i)
        score = random.random() * 25
        items[key] = score

    boosted = {4, 10, 28}

    boosted_items = []
    regular_items = []

    for i, (key, score) in enumerate(items.items()):
        if i in boosted:
            boosted_items.append(key)
            orderer.add_boosted(key)
        else:
            regular_items.append(key)
            orderer.add(key)

    sorted_items = list(orderer.sorted_by_insertion())
    assert sorted_items == boosted_items + regular_items


def test_orderer_handles_duplicate_insertions():
    orderer = Orderer()
    orderer.add_boosted("a")
    orderer.add_boosted("b")
    orderer.add_boosted("a")
    orderer.add_boosted("c")
    orderer.add("a")
    assert list(orderer.sorted_by_insertion()) == ["a", "b", "c"]


def test_merge_paragraphs_vectors():
    paragraphs = []
    for i in range(5):
        pr = ParagraphResult()
        pr.uuid = "foo"
        pr.score.bm25 = i
        pr.score.booster = 0
        pr.paragraph = f"id/text/paragraph/{i}/0-10"
        pr.start = 0
        pr.end = 10
        pr.field = "/a/title"
        paragraphs.append(pr)

    vectors = []
    for i in range(5):
        score = max(5 / float(i + 1), 1)
        vr = DocumentScored()
        vr.doc_id.id = f"id/vector/paragraph/{i}/0-2"
        vr.score = score
        vr.metadata.position.start = 0
        vr.metadata.position.start = 2
        vectors.append(vr)

    paragraphs, next_page = merge_paragraphs_vectors(
        [paragraphs], [vectors], 20, 0, min_score=1
    )
    assert not next_page
    assert len(paragraphs) == 10

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
        assert paragraphs[index].paragraph.score_type == score_type
        if score_type == SCORE_TYPE.VECTOR:
            vector_scores.add(paragraphs[index].paragraph.score)

    # Check that the vector scores are different
    assert len(vector_scores) == 5
