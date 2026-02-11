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

"""/ask endpoint + reranking tests"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from nucliadb.search.search.chat.ask import ask
from nucliadb_models.search import (
    AskRequest,
    FindRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    PredictReranker,
    RerankerName,
)


@pytest.mark.parametrize(
    "reranker,expected_reranker",
    [
        (RerankerName.PREDICT_RERANKER, RerankerName.PREDICT_RERANKER),
        (RerankerName.NOOP, RerankerName.NOOP),
        (PredictReranker(window=20), PredictReranker(window=20)),
    ],
)
async def test_ask_forwarding_rerank_options_to_find(
    reranker: RerankerName,
    expected_reranker: RerankerName,
):
    kbid = "kbid"
    find_mock = AsyncMock(return_value=(KnowledgeboxFindResults(resources={}), False, Mock(), Mock()))

    with patch("nucliadb.search.search.chat.query.find_retrieval", new=find_mock):
        ask_result = await ask(
            kbid=kbid,
            ask_request=AskRequest(
                query="my query",
                reranker=reranker,
                top_k=10,
            ),
            user_id="user-id",
            client_type=NucliaDBClientType.API,
            origin="",
            resource=None,
        )
        # we need to consume the stream response
        _ = await ask_result.json()

        assert find_mock.call_count == 1
        find_request = find_mock.call_args.args[1]
        assert isinstance(find_request, FindRequest)

        assert find_request.top_k == 10
        assert find_request.reranker == expected_reranker
