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
from nucliadb.search.api.v1.summarize import summarize_endpoint
from nucliadb.search.search.summarize import NoResourcesToSummarize
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
def dummy_request():
    return DummyTestRequest(
        scope={
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "headers": [],
        }
    )


@pytest.fixture(scope="function")
def summarize():
    with mock.patch("nucliadb.search.api.v1.summarize.summarize") as mocked:
        yield mocked


@pytest.mark.parametrize(
    "error,http_error_response",
    [
        (
            LimitsExceededError(402, "over the quota"),
            HTTPClientError(status_code=402, detail="over the quota"),
        ),
        (
            predict.ProxiedPredictAPIError(status=500, detail="Temporary error"),
            HTTPClientError(
                status_code=500,
                detail="Temporary error",
            ),
        ),
        (
            NoResourcesToSummarize(),
            HTTPClientError(
                status_code=412,
                detail="Could not summarize: No resources or extracted text found.",
            ),
        ),
    ],
)
async def test_summarize_endpoint_handles_errors(
    summarize,
    error,
    http_error_response,
    dummy_request,
):
    summarize.side_effect = error
    response = await summarize_endpoint(
        request=dummy_request,
        kbid="kbid",
        item=Mock(),
    )
    assert response.status_code == http_error_response.status_code
    assert response.body == http_error_response.body
