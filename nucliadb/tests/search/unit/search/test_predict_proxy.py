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
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.datastructures import QueryParams
from fastapi.responses import Response, StreamingResponse

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.search.search.predict_proxy import PredictProxiedEndpoints, predict_proxy
from nucliadb_models.search import NucliaDBClientType

MODULE = "nucliadb.search.search.predict_proxy"


@pytest.fixture(scope="function")
def exists_kb():
    with patch(f"{MODULE}.exists_kb", return_value=True) as mock:
        yield mock


@pytest.fixture(scope="function")
def predict_response():
    async def iter_any():
        for i in range(3):
            yield i.to_bytes(i, "big")

    resp = Mock()
    resp.status = 200
    resp.headers = {"Content-Type": "application/json"}
    resp.content = Mock(iter_any=iter_any)
    json_answer = {"answer": "foo"}
    resp.json = AsyncMock(return_value=json_answer)
    resp.read = AsyncMock(return_value=json.dumps(json_answer).encode())
    yield resp


@pytest.fixture(scope="function")
def predict(predict_response):
    predict_engine = Mock()
    predict_engine.get_predict_headers = Mock(return_value={})
    predict_engine.make_request = AsyncMock(return_value=predict_response)
    with patch(f"{MODULE}.get_predict", return_value=predict_engine):
        yield predict_engine


async def test_raises_error_on_non_existing_kb(exists_kb):
    exists_kb.return_value = False
    with pytest.raises(KnowledgeBoxNotFound):
        await predict_proxy(
            kbid="foo",
            endpoint=PredictProxiedEndpoints.CHAT,
            method="GET",
            params=QueryParams(),
            user_id="test-user",
            client_type=NucliaDBClientType.API,
            origin="test-origin",
        )


async def test_stream_response(exists_kb, predict, predict_response):
    predict_response.headers["Transfer-Encoding"] = "chunked"
    predict_response.headers["NUCLIA-LEARNING-ID"] = "foo"

    resp = await predict_proxy(
        kbid="foo",
        endpoint=PredictProxiedEndpoints.CHAT,
        method="GET",
        params=QueryParams(),
        user_id="test-user",
        client_type=NucliaDBClientType.API,
        origin="test-origin",
    )

    assert isinstance(resp, StreamingResponse)
    assert resp.status_code == 200
    assert resp.headers["NUCLIA-LEARNING-ID"] == "foo"
    assert resp.headers["Access-Control-Expose-Headers"] == "NUCLIA-LEARNING-ID"
    body = [chunk async for chunk in resp.body_iterator]
    assert list(map(lambda x: x.to_bytes(x, "big"), range(3))) == body


async def test_json_response(exists_kb, predict, predict_response):
    predict_response.headers["NUCLIA-LEARNING-ID"] = "foo"

    resp = await predict_proxy(
        kbid="foo",
        endpoint=PredictProxiedEndpoints.CHAT,
        method="GET",
        params=QueryParams(),
        user_id="test-user",
        client_type=NucliaDBClientType.API,
        origin="test-origin",
    )

    assert isinstance(resp, Response)
    assert resp.status_code == 200
    assert resp.headers["NUCLIA-LEARNING-ID"] == "foo"
    assert resp.headers["Access-Control-Expose-Headers"] == "NUCLIA-LEARNING-ID"
    assert resp.headers["Content-Type"] == "application/json"
    assert json.loads(resp.body) == {"answer": "foo"}


async def test_500_text_response(exists_kb, predict, predict_response):
    predict_response.headers["Content-Type"] = "text/plain"
    predict_response.status = 500
    predict_response.headers["NUCLIA-LEARNING-ID"] = "foo"
    predict_response.read = AsyncMock(return_value=b"foo")

    resp = await predict_proxy(
        kbid="foo",
        endpoint=PredictProxiedEndpoints.CHAT,
        method="GET",
        params=QueryParams(),
        user_id="test-user",
        client_type=NucliaDBClientType.API,
        origin="test-origin",
    )

    assert isinstance(resp, Response)
    assert resp.status_code == 500
    assert resp.headers["NUCLIA-LEARNING-ID"] == "foo"
    assert resp.headers["Access-Control-Expose-Headers"] == "NUCLIA-LEARNING-ID"
    assert resp.headers["Content-Type"].startswith("text/plain")
    assert resp.body == b"foo"
