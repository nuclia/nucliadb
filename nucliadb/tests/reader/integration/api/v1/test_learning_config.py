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
from fastapi import Response
from fastapi.responses import StreamingResponse
from httpx import AsyncClient


class MockProxy:
    def __init__(self):
        self.calls = []

    async def __call__(self, request, method, url, headers=None):
        self.calls.append((request, method, url, headers))
        if method == "GET" and "download" in url:

            async def iter_content():
                yield b"some content"

            return StreamingResponse(content=iter_content(), status_code=200)
        else:
            return Response(content=b"some content", status_code=200)


@pytest.fixture()
def learning_config_proxy_mock():
    proxy = MockProxy()
    with mock.patch("nucliadb.reader.api.v1.learning_config.learning_config_proxy", proxy):
        yield proxy


@pytest.mark.deploy_modes("component")
async def test_api(
    nucliadb_reader: AsyncClient, knowledgebox, learning_config_proxy_mock, onprem_nucliadb
):
    kbid = knowledgebox
    # Get configuration
    resp = await nucliadb_reader.get(f"/kb/{kbid}/configuration")
    assert resp.status_code == 200
    assert learning_config_proxy_mock.calls[-1][1:] == (
        "GET",
        f"/config/{kbid}",
        None,
    )

    # Download model
    resp = await nucliadb_reader.get(f"/kb/{kbid}/models/model1/path")
    assert resp.status_code == 200
    data = BytesIO()
    for chunk in resp.iter_bytes():
        data.write(chunk)
    assert data.getvalue() == b"some content"
    assert learning_config_proxy_mock.calls[-1][1:] == (
        "GET",
        f"/download/{kbid}/model/model1/path",
        None,
    )

    # List models
    resp = await nucliadb_reader.get(f"/kb/{kbid}/models")
    assert resp.status_code == 200
    assert learning_config_proxy_mock.calls[-1][1:] == (
        "GET",
        f"/models/{kbid}",
        None,
    )

    # Get metadata of a model
    resp = await nucliadb_reader.get(f"/kb/{kbid}/model/model1")
    assert resp.status_code == 200
    assert learning_config_proxy_mock.calls[-1][1:] == (
        "GET",
        f"/models/{kbid}/model/model1",
        None,
    )

    # Get schema for updates
    resp = await nucliadb_reader.get(f"/kb/{kbid}/schema", headers={"x-nucliadb-account": "account"})
    assert resp.status_code == 200
    assert learning_config_proxy_mock.calls[-1][1:] == (
        "GET",
        f"/schema/{kbid}",
        {"account-id": "account"},
    )

    # Get schema for creation
    resp = await nucliadb_reader.get("/nua/schema")
    assert resp.status_code == 200
    assert learning_config_proxy_mock.calls[-1][1:] == ("GET", "/schema", None)

    # Get models grouped by providers
    resp = await nucliadb_reader.get(f"/kb/{kbid}/generative_providers", headers={"x-nucliadb-account": "account"})
    assert resp.status_code == 200
    assert learning_config_proxy_mock.calls[-1][1:] == (
        "GET",
        f"/{kbid}/generative_providers",
        {"account-id": "account"},
    )


@pytest.mark.deploy_modes("component")
async def test_api_restricted_for_hosted(nucliadb_reader: AsyncClient, hosted_nucliadb):
    # Check that getting the creation schema does not work for hosted nucliadb
    resp = await nucliadb_reader.get("/nua/schema")
    assert resp.status_code == 404
