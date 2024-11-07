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

import pytest
from httpx import AsyncClient

from nucliadb_models.search import (
    SCORE_TYPE,
    KnowledgeboxFindResults,
    LegacyRankFusion,
    ReciprocalRankFusion,
    Reranker,
)


@pytest.mark.parametrize(
    "rank_fusion,expected_score_types",
    [
        (LegacyRankFusion().model_dump(), {SCORE_TYPE.BM25}),
        (ReciprocalRankFusion().model_dump(), {SCORE_TYPE.RANK_FUSION}),
    ],
)
async def test_rank_fusion(
    nucliadb_reader: AsyncClient,
    philosophy_books_kb: str,
    rank_fusion: dict,
    expected_score_types: set[SCORE_TYPE],
):
    kbid = philosophy_books_kb

    ask_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/ask",
        headers={
            "x-synchronous": "true",
        },
        json={
            "query": "the",
            "reranker": Reranker.NOOP,
            "rank_fusion": rank_fusion,
            "min_score": {"bm25": 0, "semantic": -10},
        },
    )
    assert ask_resp.status_code == 200

    find_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "the",
            "reranker": Reranker.NOOP,
            "rank_fusion": rank_fusion,
            "min_score": {"bm25": 0, "semantic": -10},
        },
    )
    assert find_resp.status_code == 200

    find_retrieval = KnowledgeboxFindResults.model_validate(find_resp.json())
    ask_retrieval = KnowledgeboxFindResults.model_validate(ask_resp.json()["retrieval_results"])
    assert find_retrieval == ask_retrieval
    assert len(find_retrieval.best_matches) == 7

    find_score_types = get_score_types(find_retrieval)
    ask_score_types = get_score_types(ask_retrieval)
    assert find_score_types == ask_score_types == expected_score_types


def get_score_types(results: KnowledgeboxFindResults) -> set[SCORE_TYPE]:
    score_types = set()
    for resource in results.resources.values():
        for fields in resource.fields.values():
            for paragraph in fields.paragraphs.values():
                score_types.add(paragraph.score_type)
    return score_types