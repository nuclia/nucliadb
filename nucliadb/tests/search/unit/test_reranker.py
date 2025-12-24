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

from unittest.mock import AsyncMock, Mock, patch

import pytest

from nucliadb.search.predict import ProxiedPredictAPIError, SendToPredictError
from nucliadb.search.search.rerankers import (
    PredictReranker,
    RankedItem,
    RerankableItem,
    Reranker,
    RerankingOptions,
    apply_reranking,
    sort_by_score,
)
from nucliadb_models.internal.predict import RerankResponse
from nucliadb_models.search import (
    SCORE_TYPE,
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
)


class DummyReranker(Reranker):
    def __init__(self, window: int):
        self._window = window

    @property
    def window(self) -> int | None:
        return self._window

    async def _rerank(self, items: list[RerankableItem], options: RerankingOptions) -> list[RankedItem]:
        reranked = [
            RankedItem(
                id=item.id,
                score=item.score,
                score_type=item.score_type,
            )
            for item in items
        ]
        sort_by_score(reranked)
        return reranked


async def test_rerank_find_response():
    """Validate manipulation of find response by the reranker"""
    reranker = DummyReranker(window=3)

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

    to_rerank = [
        RerankableItem(
            id=paragraph_id,
            score=paragraph.score,
            score_type=paragraph.score_type,
            content=paragraph.text,
        )
        for resource in find_response.resources.values()
        for field in resource.fields.values()
        for paragraph_id, paragraph in field.paragraphs.items()
    ]
    reranked = await reranker.rerank(to_rerank, RerankingOptions(kbid="kbid", query="my query"))
    apply_reranking(find_response, reranked)

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


async def test_predict_reranker_dont_call_predict_with_empty_results():
    predict = AsyncMock()
    predict.rerank.return_value = RerankResponse(context_scores={"id": 10})

    with patch(
        "nucliadb.search.search.rerankers.get_predict", new=Mock(return_value=predict)
    ) as get_predict:
        reranker = PredictReranker(window=20)

        await reranker.rerank(items=[], options=Mock())
        assert get_predict.call_count == 0

        reranked = await reranker.rerank(
            items=[RerankableItem(id="id", score=1, score_type=SCORE_TYPE.VECTOR, content="bla bla")],
            options=RerankingOptions(kbid="kbid", query="my query"),
        )
        assert get_predict.call_count == 1

        assert len(reranked) == 1
        assert reranked[0].id == "id"
        assert reranked[0].score == 10
        assert reranked[0].score_type == SCORE_TYPE.RERANKER


@pytest.mark.parametrize("error", [SendToPredictError(), ProxiedPredictAPIError(status=500)])
async def test_predict_reranker_handles_predict_failures(
    error: Exception,
):
    predict = AsyncMock()
    predict.rerank.side_effect = error

    with patch(
        "nucliadb.search.search.rerankers.get_predict", new=Mock(return_value=predict)
    ) as get_predict:
        reranker = PredictReranker(window=20)

        await reranker.rerank(items=[], options=Mock())
        assert get_predict.call_count == 0

        reranked = await reranker.rerank(
            items=[
                RerankableItem(id="1", score=1, score_type=SCORE_TYPE.BM25, content="bla bla"),
                RerankableItem(id="2", score=0.6, score_type=SCORE_TYPE.VECTOR, content="bla bla"),
                RerankableItem(id="3", score=1.5, score_type=SCORE_TYPE.BOTH, content="bla bla"),
            ],
            options=RerankingOptions(kbid="kbid", query="my query"),
        )
        assert get_predict.call_count == 1

        assert len(reranked) == 3
        assert reranked[0] == RankedItem(id="3", score=1.5, score_type=SCORE_TYPE.BOTH)
        assert reranked[1] == RankedItem(id="1", score=1, score_type=SCORE_TYPE.BM25)
        assert reranked[2] == RankedItem(id="2", score=0.6, score_type=SCORE_TYPE.VECTOR)
