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
    get_find_results,
    parse_audit_answer,
)
from nucliadb_models.search import (
    ChatOptions,
    ChatRequest,
    KnowledgeboxFindResults,
    MinScore,
    NucliaDBClientType,
    SearchOptions,
)


@pytest.fixture()
def predict():
    predict = mock.AsyncMock()
    with mock.patch("nucliadb.search.search.chat.query.get_predict", return_value=predict):
        yield predict


@pytest.mark.parametrize(
    "chat_features,find_features",
    [
        (
            None,  # default value will be used
            [SearchOptions.SEMANTIC, SearchOptions.KEYWORD, SearchOptions.RELATIONS],
        ),
        (
            [ChatOptions.KEYWORD, ChatOptions.SEMANTIC, ChatOptions.RELATIONS],
            [SearchOptions.KEYWORD, SearchOptions.SEMANTIC, SearchOptions.RELATIONS],
        ),
        (
            [ChatOptions.KEYWORD, ChatOptions.SEMANTIC],
            [
                SearchOptions.KEYWORD,
                SearchOptions.SEMANTIC,
            ],
        ),
        (
            [ChatOptions.SEMANTIC],
            [
                SearchOptions.SEMANTIC,
            ],
        ),
        (
            [ChatOptions.KEYWORD],
            [
                SearchOptions.KEYWORD,
            ],
        ),
    ],
)
async def test_get_find_results_vector_search_is_optional(predict, chat_features, find_features):
    find_results = KnowledgeboxFindResults(
        total=0, min_score=MinScore(semantic=0.7), resources={}, facets=[]
    )

    chat_request = ChatRequest(query="query")
    if chat_features is not None:
        chat_request.features = chat_features

    query_parser = mock.AsyncMock()

    with mock.patch(
        "nucliadb.search.search.chat.query.find",
        return_value=(find_results, False, query_parser),
    ) as find_mock:
        await get_find_results(
            kbid="kbid",
            query="query",
            chat_request=chat_request,
            ndb_client=NucliaDBClientType.API,
            user="user_id",
            origin="origin",
        )
        find_request = find_mock.call_args[0][1]
        assert set(find_request.features) == set(find_features)


@pytest.mark.parametrize(
    "raw_text_answer,status_code,audit_answer",
    [
        (b"foobar", AnswerStatusCode.NO_CONTEXT, None),
        (b"foobar", AnswerStatusCode.SUCCESS, "foobar"),
    ],
)
def test_parse_audit_answer(raw_text_answer, status_code, audit_answer):
    assert parse_audit_answer(raw_text_answer, status_code) == audit_answer
