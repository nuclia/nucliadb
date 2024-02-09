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

from nucliadb import learning_config
from nucliadb_models.resource import NucliaDBRoles


class MockProxy:
    def __init__(self):
        self.calls = []

    async def __call__(self, request, method, url):
        self.calls.append((request, method, url))
        return Response(status_code=204)


@pytest.fixture()
def learning_config_proxy():
    proxy = MockProxy()
    with mock.patch.object(learning_config, "proxy", proxy):
        yield proxy


async def test_api(writer_api, knowledgebox_ingest, learning_config_proxy):
    kbid = knowledgebox_ingest
    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        # Get configuration
        resp = await client.patch(f"/kb/{kbid}/configuration", json={"some": "data"})
        assert resp.status_code == 204

        assert learning_config_proxy.calls[0][1:] == ("POST", f"/config/{kbid}")
