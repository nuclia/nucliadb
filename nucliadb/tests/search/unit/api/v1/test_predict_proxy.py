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
import json
from unittest import mock
from unittest.mock import Mock

import pytest
from httpx import ASGITransport, AsyncClient
from starlette.requests import Request

from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.predict_proxy import predict_proxy_endpoint
from nucliadb.search.search.predict_proxy import PredictProxiedEndpoints
from nucliadb_utils.exceptions import LimitsExceededError
from tests.utils.aiohttp_session import get_mocked_session

MODULE = "nucliadb.search.api.v1.predict_proxy"


async def test_remi_proxy_sends_authenticated_account():
    from nucliadb.search.app import application

    kbid = "bc717c30-010f-490f-ad22-b8ecbbeb4587"
    engine = predict.PredictEngine(cluster_url="http://predict", onprem=False)
    engine.session = get_mocked_session(
        "POST",
        200,
        read='{"context_relevance":[5],"groundedness":[5]}',
        headers={"Content-Type": "application/json"},
        context_manager=False,
    )
    with (
        mock.patch("nucliadb.search.search.predict_proxy.exists_kb", return_value=True),
        mock.patch("nucliadb.search.search.predict_proxy.get_predict", return_value=engine),
    ):
        async with AsyncClient(
            transport=ASGITransport(app=application), base_url="http://test"
        ) as client:
            response = await client.post(
                f"/api/v1/kb/{kbid}/predict/remi",
                headers={
                    "X-NUCLIADB-ROLES": "READER",
                    "X-NUCLIADB-ACCOUNT": "authenticated-account",
                    "X-STF-ACCOUNT": "untrusted-account",
                },
                json={
                    "user_id": "test-user",
                    "question": "What is the capital of France?",
                    "answer": "Paris.",
                    "contexts": ["Paris is the capital of France."],
                },
            )

    assert response.status_code == 200
    assert engine.session.post.call_args.kwargs["headers"] == {
        "X-STF-KBID": kbid,
        "X-STF-ACCOUNT": "authenticated-account",
        "accept": "*/*",
    }


class DummyTestRequest(Request):
    @property
    def auth(self):
        return Mock(scopes=["READER"])

    @property
    def user(self):
        return Mock(display_name="username")

    async def json(self):
        raise json.JSONDecodeError("test", "test", 0)


@pytest.fixture(scope="function")
def dummy_request():
    return DummyTestRequest(
        scope={
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "headers": [],
            "query_string": "",
        }
    )


@pytest.fixture(scope="function")
def predict_proxy():
    with mock.patch(f"{MODULE}.predict_proxy") as mocked:
        yield mocked


@pytest.mark.parametrize(
    "predict_error,http_error_response",
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
    ],
)
async def test_predict_proxy_endpoint_error_handling(
    predict_proxy,
    predict_error,
    http_error_response,
    dummy_request,
):
    predict_proxy.side_effect = predict_error
    response = await predict_proxy_endpoint(
        request=dummy_request,
        kbid="kbid",
        endpoint=PredictProxiedEndpoints.CHAT,
    )
    assert response.status_code == http_error_response.status_code
