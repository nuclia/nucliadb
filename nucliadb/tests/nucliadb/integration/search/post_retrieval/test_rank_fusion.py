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
from pytest_mock import MockerFixture

from nucliadb.search.search import find, find_merge
from nucliadb_models.search import (
    SCORE_TYPE,
    KnowledgeboxFindResults,
    ReciprocalRankFusion,
    RerankerName,
)


@pytest.mark.parametrize(
    "rank_fusion,expected_score_types",
    [
        # TODO: use better data and add here BOTH and VECTOR
        (ReciprocalRankFusion().model_dump(), {SCORE_TYPE.BM25}),
    ],
)
@pytest.mark.deploy_modes("standalone")
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
            "reranker": RerankerName.NOOP,
            "rank_fusion": rank_fusion,
            "min_score": {"bm25": 0, "semantic": -10},
        },
    )
    assert ask_resp.status_code == 200

    find_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "the",
            "reranker": RerankerName.NOOP,
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


@pytest.mark.deploy_modes("standalone")
async def test_reciprocal_rank_fusion_requests_more_results(
    nucliadb_reader: AsyncClient,
    philosophy_books_kb: str,
    mocker: MockerFixture,
):
    """Validate predict reranker asks for more results than the user and we send
    them to Predict API.

    """
    kbid = philosophy_books_kb

    spy_build_find_response = mocker.spy(find, "build_find_response")
    spy_cut_page = mocker.spy(find_merge, "cut_page")

    payload = {
        "query": "the",
        "min_score": {"bm25": 0, "semantic": -10},
        "rank_fusion": {
            "name": "rrf",
            "window": 5,
        },
        "reranker": "noop",
        "top_k": 3,
    }

    ask_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/ask",
        headers={
            "x-synchronous": "true",
        },
        json=payload,
    )
    assert ask_resp.status_code == 200

    find_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json=payload,
    )
    assert find_resp.status_code == 200

    assert spy_build_find_response.call_count == 2
    assert spy_cut_page.call_count == 2

    for i in range(spy_build_find_response.call_count):
        build_find_response_call = spy_build_find_response.call_args_list[i]
        cut_page_call = spy_cut_page.call_args_list[i]

        merged_search_response = build_find_response_call.args[0]
        assert len(merged_search_response.paragraph.results) == 5

        items, top_k = cut_page_call.args
        assert len(items) == 5
        assert top_k == 3

    find_retrieval = KnowledgeboxFindResults.model_validate(find_resp.json())
    ask_retrieval = KnowledgeboxFindResults.model_validate(ask_resp.json()["retrieval_results"])
    assert find_retrieval == ask_retrieval
    assert len(find_retrieval.best_matches) == 3
