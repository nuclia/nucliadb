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

from unittest.mock import patch

from nucliadb.search.search.rerankers import PredictReranker, RankedItem, RerankableItem, Reranker
from nucliadb_models.search import (
    SCORE_TYPE,
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
)


class NoopReranker(Reranker):
    @property
    def needs_extra_results(self) -> bool:
        return False

    def items_needed(self, requested: int, shards: int = 1) -> int:
        return requested

    async def rerank(self, kbid: str, query: str, items: list[RerankableItem]) -> list[RankedItem]:
        return [
            RankedItem(
                id=item.id,
                score=item.score,
                score_type=item.score_type,
            )
            for item in items
        ]


async def test_rerank_find_response():
    """Validate manipulation of find response by the reranker"""
    reranker: Reranker = NoopReranker()

    find_response = KnowledgeboxFindResults(
        resources={
            "resource-A": FindResource(
                id="resource-A",
                fields={
                    "field-A": FindField(
                        paragraphs={
                            "paragraph-1": FindParagraph(
                                id="paragraph-1",
                                score=0.1,
                                score_type=SCORE_TYPE.VECTOR,
                                text="1",
                            ),
                            "paragraph-2": FindParagraph(
                                id="paragraph-2",
                                score=0.25,
                                score_type=SCORE_TYPE.BM25,
                                text="2",
                            ),
                            "paragraph-3": FindParagraph(
                                id="paragraph-3",
                                score=0.15,
                                score_type=SCORE_TYPE.VECTOR,
                                text="3",
                            ),
                        }
                    )
                },
            )
        },
        page_number=0,
        page_size=20,
    )

    await reranker.rerank_find_response("kbid", "my query", find_response)

    import json

    print(json.dumps(find_response.model_dump(), indent=4))

    ranking = []
    for resource in find_response.resources.values():
        for field in resource.fields.values():
            for paragraph in field.paragraphs.values():
                ranking.append((paragraph.order, paragraph.score, paragraph.score_type, paragraph.id))
    ranking.sort()

    expected = [
        (0, 0.25, SCORE_TYPE.BM25, "paragraph-2"),
        (1, 0.15, SCORE_TYPE.VECTOR, "paragraph-3"),
        (2, 0.1, SCORE_TYPE.VECTOR, "paragraph-1"),
    ]
    assert ranking == expected
    assert find_response.best_matches == [x[3] for x in expected]


def test_predict_reranker_dont_call_predict_with_empty_results():
    with patch("nucliadb.search.search.rerankers.get_predict") as get_predict:
        reranker = PredictReranker()

        reranker.rerank("kbid", "my query", items=[])
        assert get_predict.call_count == 0

        reranker.rerank(
            "kbid", "my query", items=[RerankableItem(id="id", score=1, score_type=SCORE_TYPE.VECTOR)]
        )
        assert get_predict.call_count == 1
