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
from unittest import mock
from unittest.mock import Mock

import pytest
from starlette.requests import Request

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.ask import ask_knowledgebox_endpoint
from nucliadb.search.search.exceptions import (
    IncompleteFindResultsError,
    InvalidQueryError,
)
from nucliadb_utils.exceptions import LimitsExceededError

pytestmark = pytest.mark.asyncio


class DummyTestRequest(Request):
    @property
    def auth(self):
        return Mock(scopes=["READER"])

    @property
    def user(self):
        return Mock(display_name="username")


@pytest.fixture(scope="function")
def ask_mock():
    with mock.patch(
        "nucliadb.search.api.v1.ask.ask",
    ) as mocked:
        yield mocked


@pytest.mark.parametrize(
    "ask_error,http_error_response",
    [
        (
            KnowledgeBoxNotFound(),
            HTTPClientError(status_code=404, detail="Knowledge Box not found."),
        ),
        (
            LimitsExceededError(402, "over the quota"),
            HTTPClientError(status_code=402, detail="over the quota"),
        ),
        (
            predict.ProxiedPredictAPIError(status=999, detail="foo"),
            HTTPClientError(status_code=999, detail="foo"),
        ),
        (
            IncompleteFindResultsError(),
            HTTPClientError(
                status_code=529,
                detail="Temporary error on information retrieval. Please try again.",
            ),
        ),
        (
            predict.RephraseMissingContextError(),
            HTTPClientError(
                status_code=412,
                detail="Unable to rephrase the query with the provided context.",
            ),
        ),
        (
            predict.RephraseError("foobar"),
            HTTPClientError(
                status_code=529,
                detail="Temporary error while rephrasing the query. Please try again later. Error: foobar",
            ),
        ),
        (
            InvalidQueryError("foobar", "baz"),
            HTTPClientError(
                status_code=412, detail="Invalid query. Error in foobar: baz"
            ),
        ),
    ],
)
async def test_ask_endpoint_handles_errors(ask_mock, ask_error, http_error_response):
    ask_mock.side_effect = ask_error
    request = DummyTestRequest(
        scope={
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "headers": [],
        }
    )
    response = await ask_knowledgebox_endpoint(
        request=request,
        kbid="kbid",
        item=Mock(),
        x_ndb_client=None,
        x_nucliadb_user="",
        x_forwarded_for="",
    )
    assert response.status_code == http_error_response.status_code
    assert response.body == http_error_response.body
