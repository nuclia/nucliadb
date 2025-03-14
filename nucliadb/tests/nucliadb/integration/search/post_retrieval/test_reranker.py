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

import pytest
from httpx import AsyncClient
from pytest_mock import MockerFixture

from nucliadb.search.search import find, find_merge
from nucliadb_models.search import KnowledgeboxFindResults, PredictReranker, RerankerName


@pytest.mark.parametrize(
    "reranker",
    [
        RerankerName.PREDICT_RERANKER,
        RerankerName.NOOP,
        PredictReranker(window=50).model_dump(),
    ],
)
@pytest.mark.deploy_modes("standalone")
async def test_reranker(
    nucliadb_reader: AsyncClient,
    philosophy_books_kb: str,
    reranker: str,
):
    kbid = philosophy_books_kb

    ask_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/ask",
        headers={
            "x-synchronous": "true",
        },
        json={
            "query": "the",
            "reranker": reranker,
            "min_score": {"bm25": 0, "semantic": -10},
        },
    )
    assert ask_resp.status_code == 200

    find_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "the",
            "reranker": reranker,
            "min_score": {"bm25": 0, "semantic": -10},
        },
    )
    assert find_resp.status_code == 200

    find_retrieval = KnowledgeboxFindResults.model_validate(find_resp.json())
    ask_retrieval = KnowledgeboxFindResults.model_validate(ask_resp.json()["retrieval_results"])
    assert find_retrieval == ask_retrieval
    assert len(find_retrieval.best_matches) == 7


@pytest.mark.parametrize(
    "reranker,extra",
    [
        (RerankerName.PREDICT_RERANKER, 5 * 2),
        (PredictReranker(window=5 * 2).model_dump(), 5 * 2),
    ],
)
@pytest.mark.deploy_modes("standalone")
async def test_predict_reranker_requests_more_results(
    nucliadb_reader: AsyncClient,
    philosophy_books_kb: str,
    mocker: MockerFixture,
    reranker: Union[RerankerName, PredictReranker],
    extra: int,
):
    """Validate predict reranker asks for more results than the user and we send
    them to Predict API.

    """
    kbid = philosophy_books_kb

    spy_build_find_response = mocker.spy(find, "build_find_response")
    spy_cut_page = mocker.spy(find_merge, "cut_page")

    payload = {
        "query": "the",
        "reranker": reranker,
        "min_score": {"bm25": 0, "semantic": -10},
        "top_k": 5,
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

        search_responses = build_find_response_call.args[0]
        assert len(search_responses) == 1

        search_response = search_responses[0]
        assert len(search_response.paragraph.results) == 7

        items, top_k = cut_page_call.args
        assert len(items) == 7
        assert top_k == extra

    find_retrieval = KnowledgeboxFindResults.model_validate(find_resp.json())
    ask_retrieval = KnowledgeboxFindResults.model_validate(ask_resp.json()["retrieval_results"])
    assert find_retrieval == ask_retrieval
    assert len(find_retrieval.best_matches) == 5
