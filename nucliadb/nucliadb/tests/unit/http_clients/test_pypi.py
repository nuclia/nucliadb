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

from nucliadb.common.http_clients import pypi


class TestPyPi:
    @pytest.fixture()
    def response(self):
        resp = mock.Mock()
        resp.status = 200
        resp.json.return_value = {"info": {"version": "1.0.0"}}
        yield resp

    @pytest.fixture()
    def client(self, response):
        cl = pypi.PyPi()
        cl.session = mock.MagicMock()
        cl.session.get = mock.AsyncMock()
        cl.session.aclose = mock.AsyncMock()
        cl.session.get.return_value = response
        yield cl

    @pytest.mark.asyncio
    async def test_get_latest_version(self, client):
        assert await client.get_latest_version("foo") == "1.0.0"

    @pytest.mark.asyncio
    async def test_context_manager(self, client):
        async with client:
            pass
        client.session.aclose.assert_awaited_once()
