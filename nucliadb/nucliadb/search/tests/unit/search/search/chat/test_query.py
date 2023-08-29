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
from unittest import mock

import pytest

from nucliadb.search.predict import AnswerStatusCode
from nucliadb.search.search.chat.query import (
    _parse_answer_status_code,
    async_gen_lookahead,
    chat,
)
from nucliadb_models.search import (
    ChatRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
)


@pytest.fixture()
def predict():
    predict = mock.AsyncMock()
    with mock.patch(
        "nucliadb.search.search.chat.query.get_predict", return_value=predict
    ):
        yield predict


async def test_chat_does_not_call_predict_if_no_find_results(
    predict,
):
    find_results = KnowledgeboxFindResults(
        total=0, min_score=0.7, resources={}, facets=[]
    )
    chat_request = ChatRequest(query="query")

    await chat(
        "kbid",
        "query",
        None,
        find_results,
        chat_request,
        "user_id",
        NucliaDBClientType.API,
        "origin",
    )

    predict.chat_query.assert_not_called()


async def test_async_gen_lookahead():
    async def gen(n):
        for i in range(n):
            yield i

    assert [item async for item in async_gen_lookahead(gen(0))] == []
    assert [item async for item in async_gen_lookahead(gen(1))] == [(0, True)]
    assert [item async for item in async_gen_lookahead(gen(2))] == [
        (0, False),
        (1, True),
    ]


@pytest.mark.parametrize(
    "chunk,status_code,error",
    [
        (b"", None, True),
        (b"errorcodeisnotpresetn", None, True),
        (b"0", AnswerStatusCode.SUCCESS, False),
        (b"-1", AnswerStatusCode.ERROR, False),
        (b"-2", AnswerStatusCode.NO_CONTEXT, False),
        (b"foo.0", AnswerStatusCode.SUCCESS, False),
        (b"bar.-1", AnswerStatusCode.ERROR, False),
        (b"baz.-2", AnswerStatusCode.NO_CONTEXT, False),
    ],
)
def test_parse_status_code(chunk, status_code, error):
    if error:
        with pytest.raises(ValueError):
            _parse_answer_status_code(chunk)
    else:
        assert _parse_answer_status_code(chunk) == status_code
