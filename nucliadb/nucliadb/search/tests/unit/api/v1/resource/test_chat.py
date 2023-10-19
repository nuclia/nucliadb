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

from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.resource.chat import resource_chat_endpoint
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
def create_chat_response_mock():
    with mock.patch(
        "nucliadb.search.api.v1.resource.chat.create_chat_response",
    ) as mocked:
        yield mocked


@pytest.mark.parametrize(
    "predict_error,http_error_response",
    [
        (
            LimitsExceededError(402, "over the quota"),
            HTTPClientError(status_code=402, detail="over the quota"),
        ),
        (
            predict.RephraseError("foobar"),
            HTTPClientError(
                status_code=529,
                detail="Temporary error while rephrasing the query. Please try again later. Error: foobar",
            ),
        ),
        (
            predict.RephraseMissingContextError(),
            HTTPClientError(
                status_code=412,
                detail="Unable to rephrase the query with the provided context.",
            ),
        ),
    ],
)
async def test_resource_chat_endpoint_handles_errors(
    create_chat_response_mock, predict_error, http_error_response
):
    create_chat_response_mock.side_effect = predict_error
    request = DummyTestRequest(
        scope={
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "headers": [],
        }
    )
    response = await resource_chat_endpoint(
        request=request,
        kbid="kbid",
        rid="rid",
        item=Mock(),
        x_ndb_client=None,
        x_nucliadb_user="",
        x_forwarded_for="",
    )
    assert response.status_code == http_error_response.status_code
    assert response.body == http_error_response.body
