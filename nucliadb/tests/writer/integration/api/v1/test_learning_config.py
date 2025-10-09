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

import pytest
from fastapi import Response
from httpx import AsyncClient


class MockProxy:
    def __init__(self):
        self.calls = []

    async def __call__(self, request, method, url, headers={}):
        self.calls.append((request, method, url, headers))
        return Response(status_code=204)


@pytest.fixture()
def learning_config_proxy_mock():
    proxy = MockProxy()
    with mock.patch("nucliadb.writer.api.v1.learning_config.learning_config_proxy", proxy):
        yield proxy


@pytest.mark.deploy_modes("component")
async def test_api(
    nucliadb_writer: AsyncClient, knowledgebox: str, learning_config_proxy_mock: MockProxy
):
    kbid = knowledgebox

    # post configuration
    resp = await nucliadb_writer.post(f"/kb/{kbid}/configuration", json={"some": "data"})
    assert resp.status_code == 204

    assert learning_config_proxy_mock.calls[0][1:] == ("POST", f"/config/{kbid}", {})

    # patch configuration
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/configuration", json={"some": "data"}, headers={"x-nucliadb-account": "account"}
    )
    assert resp.status_code == 204

    assert learning_config_proxy_mock.calls[1][1:] == (
        "PATCH",
        f"/config/{kbid}",
        {"account-id": "account"},
    )
