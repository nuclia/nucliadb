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

from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.datastructures import QueryParams
from fastapi.responses import JSONResponse, StreamingResponse

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.search.search.predict_proxy import PredictProxiedEndpoints, predict_proxy

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
    resp.headers = {}
    resp.content = Mock(iter_any=iter_any)
    resp.json = AsyncMock(return_value={"answer": "foo"})
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
            "foo",
            PredictProxiedEndpoints.CHAT,
            "GET",
            QueryParams(),
        )


async def test_stream_response(exists_kb, predict, predict_response):
    predict_response.headers["Transfer-Encoding"] = "chunked"
    predict_response.headers["NUCLIA-LEARNING-ID"] = "foo"

    resp = await predict_proxy(
        "foo",
        PredictProxiedEndpoints.CHAT,
        "GET",
        QueryParams(),
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
        "foo",
        PredictProxiedEndpoints.CHAT,
        "GET",
        QueryParams(),
    )

    assert isinstance(resp, JSONResponse)
    assert resp.status_code == 200
    assert resp.headers["NUCLIA-LEARNING-ID"] == "foo"
    assert resp.headers["Access-Control-Expose-Headers"] == "NUCLIA-LEARNING-ID"
    assert resp.body == b'{"answer":"foo"}'
