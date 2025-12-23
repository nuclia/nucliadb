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
from unittest.mock import patch

import pytest
from httpx import AsyncClient
from pytest_mock import MockerFixture

from nucliadb.search.search import find, find_merge
from nucliadb.search.search.chat import query
from nucliadb.search.search.chat.query import rpc
from nucliadb.search.utilities import get_predict
from nucliadb_models.retrieval import RetrievalRequest
from nucliadb_models.search import KnowledgeboxFindResults, PredictReranker, RerankerName
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.featureflagging import Settings
from tests.ndbfixtures.resources import smb_wonder_resource


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
    assert find_retrieval.resources == ask_retrieval.resources
    assert find_retrieval.best_matches == ask_retrieval.best_matches
    assert len(find_retrieval.best_matches) == 7


@pytest.mark.skipif(Settings().disable_ask_decoupled_ff, reason="refactored spy")
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

    payload = {
        "query": "the",
        "reranker": reranker,
        "min_score": {"bm25": 0, "semantic": -10},
        "top_k": 5,
    }

    # Test /ask

    spy_retrieve = mocker.spy(rpc, "retrieve")
    spy_augment_and_rerank = mocker.spy(query, "augment_and_rerank")

    ask_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/ask",
        headers={
            "x-synchronous": "true",
        },
        json=payload,
    )
    assert ask_resp.status_code == 200

    assert spy_retrieve.call_count == 1
    assert isinstance(spy_retrieve.call_args.args[1], RetrievalRequest)
    req: RetrievalRequest = spy_retrieve.call_args.args[1]
    assert req.top_k == extra

    assert spy_augment_and_rerank.call_count == 1
    assert len(spy_augment_and_rerank.call_args.args[1]) == 7
    assert spy_augment_and_rerank.call_args.kwargs["top_k"] == 5

    # Test /find

    spy_build_find_response = mocker.spy(find, "build_find_response")
    spy_cut_page = mocker.spy(find_merge, "cut_page")

    find_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json=payload,
    )
    assert find_resp.status_code == 200

    assert spy_build_find_response.call_count == 1
    assert spy_cut_page.call_count == 1

    merged_search_response = spy_build_find_response.call_args.args[0]
    assert len(merged_search_response.paragraph.results) == 7

    items, top_k = spy_cut_page.call_args.args
    assert len(items) == 7
    assert top_k == extra

    # Validate both return the same retrieval results

    find_retrieval = KnowledgeboxFindResults.model_validate(find_resp.json())
    ask_retrieval = KnowledgeboxFindResults.model_validate(ask_resp.json()["retrieval_results"])
    assert find_retrieval.resources == ask_retrieval.resources
    assert find_retrieval.best_matches == ask_retrieval.best_matches
    assert len(find_retrieval.best_matches) == 5


@pytest.mark.parametrize(
    "features",
    [
        ["keyword"],
        ["semantic"],
        ["keyword", "semantic"],
    ],
)
@pytest.mark.deploy_modes("standalone")
async def test_predict_reranker_options(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
    mocker: MockerFixture,
    features: list[str],
):
    kbid = knowledgebox
    await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    predict = get_predict()
    spy_rerank = mocker.spy(predict, "rerank")

    query = "a super query"
    find_resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": query,
            "features": features,
            "reranker": "predict",
            "min_score": {"bm25": 0, "semantic": -10},
        },
    )
    assert find_resp.status_code == 200

    assert spy_rerank.call_count == 1
    assert spy_rerank.call_args.args[1].question == query

    spy_rerank.reset_mock()
    rephrased_query = "a super rephrased query"
    with patch("nucliadb.search.search.find.get_rephrased_query", return_value=rephrased_query):
        find_resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json={
                "query": query,
                "features": features,
                "reranker": "predict",
                "min_score": {"bm25": 0, "semantic": -10},
            },
        )
        assert find_resp.status_code == 200

        assert spy_rerank.call_count == 1
        assert spy_rerank.call_args.args[1].question == rephrased_query
