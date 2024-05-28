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
from io import BytesIO
from unittest import mock

import pytest

from nucliadb.learning_proxy import (
    LearningConfiguration,
    LearningService,
    delete_configuration,
    get_configuration,
    learning_config_client,
    proxy,
    set_configuration,
)

MODULE = "nucliadb.learning_proxy"


@pytest.fixture()
def config_response():
    resp = mock.Mock(
        status_code=200,
        headers={
            "x-foo": "bar",
            "Content-Type": "application/json",
        },
        content=b"some data",
    )
    resp.raise_for_status.return_value = None
    yield resp


@pytest.fixture()
def config_stream_response():
    resp = mock.Mock(
        status_code=200,
        headers={
            "Transfer-Encoding": "chunked",
        },
        content=b"some data",
    )

    async def aiter_bytes():
        yield b"some data"

    resp.aiter_bytes = aiter_bytes
    resp.raise_for_status.return_value = None
    yield resp


@pytest.fixture()
def async_client(config_response):
    client = mock.AsyncMock()
    client.is_closed.return_value = False
    client.request = mock.AsyncMock(return_value=config_response)
    client.get = mock.AsyncMock(return_value=config_response)
    client.post = mock.AsyncMock(return_value=config_response)
    client.patch = mock.AsyncMock(return_value=config_response)
    client.delete = mock.AsyncMock(return_value=config_response)
    with mock.patch(f"{MODULE}.service_client") as mocked:
        mocked.return_value.__aenter__.return_value = client
        mocked.return_value.__aexit__.return_value = None
        yield client


@pytest.fixture()
def is_onprem_nucliadb_mock():
    with mock.patch(f"{MODULE}.is_onprem_nucliadb", return_value=False) as mocked:
        yield mocked


@pytest.fixture()
def settings():
    with mock.patch(f"{MODULE}.nuclia_settings") as settings:
        settings.learning_internal_svc_base_url = (
            "http://{service}.learning.svc.cluster.local:8080"
        )
        settings.nuclia_service_account = "service-account"
        settings.nuclia_zone = "europe-1"
        settings.nuclia_public_url = "http://{zone}.public-url"
        yield settings


async def test_get_learning_config_client(settings, is_onprem_nucliadb_mock):
    is_onprem_nucliadb_mock.return_value = True
    async with learning_config_client() as client:
        assert str(client.base_url) == "http://europe-1.public-url/api/v1/"
        assert client.headers["X-NUCLIA-NUAKEY"] == f"Bearer service-account"

    is_onprem_nucliadb_mock.return_value = False
    async with learning_config_client() as client:
        assert (
            str(client.base_url)
            == "http://config.learning.svc.cluster.local:8080/api/v1/internal/"
        )
        assert "X-NUCLIA-NUAKEY" not in client.headers


async def test_get_configuration(async_client):
    lconfig = LearningConfiguration(
        semantic_model="multilingual",
        semantic_threshold=1.5,
        semantic_vector_size=222,
        semantic_vector_similarity="cosine",
    )
    resp = mock.Mock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = lconfig.dict()
    async_client.get.return_value = resp

    resp = await get_configuration("kbid")

    assert resp is not None
    assert resp == lconfig
    async_client.get.assert_called_once_with("config/kbid")


async def test_set_configuration(config_response, async_client):
    config_response.json.return_value = {
        "semantic_model": "multilingual",
        "semantic_threshold": 1.5,
        "semantic_vector_size": 222,
        "semantic_vector_similarity": "cosine",
    }
    resp = await set_configuration("kbid", {"some": "data"})
    assert resp is not None
    async_client.post.assert_called_once_with("config/kbid", json={"some": "data"})


async def test_delete_configuration(async_client):
    resp = await delete_configuration("kbid")
    assert resp is None
    async_client.delete.assert_called_once_with("config/kbid")


async def test_proxy(async_client):
    request = mock.Mock(
        query_params={"some": "data"},
        body=mock.AsyncMock(return_value=b"some data"),
        headers={"x-nucliadb-user": "user", "x-nucliadb-roles": "roles"},
    )
    response = await proxy(
        LearningService.CONFIG, request, "GET", "url", extra_headers={"foo": "bar"}
    )

    assert response.status_code == 200
    assert response.body == b"some data"
    assert response.media_type == "application/json"
    assert response.headers["x-foo"] == "bar"

    async_client.request.assert_called_once_with(
        method="GET",
        url="url",
        params={"some": "data"},
        content=b"some data",
        headers={"foo": "bar", "x-nucliadb-user": "user", "x-nucliadb-roles": "roles"},
    )


async def test_proxy_stream_response(async_client, config_stream_response):
    async_client.request.return_value = config_stream_response

    request = mock.Mock(
        query_params={"some": "data"},
        body=mock.AsyncMock(return_value=b"some data"),
        headers={"x-nucliadb-user": "user", "x-nucliadb-roles": "roles"},
    )
    response = await proxy(LearningService.CONFIG, request, "GET", "url")

    assert response.status_code == 200
    data = BytesIO()
    async for chunk in response.body_iterator:
        data.write(chunk)
    assert data.getvalue() == b"some data"
    assert response.headers["Transfer-Encoding"] == "chunked"

    async_client.request.assert_called_once_with(
        method="GET",
        url="url",
        params={"some": "data"},
        content=b"some data",
        headers={"x-nucliadb-user": "user", "x-nucliadb-roles": "roles"},
    )


async def test_proxy_error(async_client):
    async_client.request.side_effect = Exception("some error")

    request = mock.Mock(
        query_params={"some": "data"},
        body=mock.AsyncMock(return_value=b"some data"),
        headers={},
    )
    response = await proxy(LearningService.CONFIG, request, "GET", "url")

    assert response.status_code == 503
    assert (
        response.body
        == b"Unexpected error while trying to proxy the request to the learning config API."
    )
    assert response.media_type == "text/plain"

    assert len(async_client.request.mock_calls) == 3
    async_client.request.assert_called_with(
        method="GET",
        url="url",
        params={"some": "data"},
        content=b"some data",
        headers={},
    )


async def test_proxy_headers(async_client):
    request = mock.Mock(
        query_params={},
        body=mock.AsyncMock(return_value=b""),
        headers={
            "x-nucliadb-user": "user",
            "x-nucliadb-roles": "roles",
            "host": "localhost",
        },
    )
    response = await proxy(LearningService.CONFIG, request, "GET", "url")

    assert response.status_code == 200

    async_client.request.assert_called_once_with(
        method="GET",
        url="url",
        params={},
        content=b"",
        headers={"x-nucliadb-user": "user", "x-nucliadb-roles": "roles"},
    )
