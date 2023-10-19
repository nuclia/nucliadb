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

from unittest.mock import Mock

import orjson
import pytest
from fastapi import Request

from nucliadb.standalone import api_router
from nucliadb.standalone.settings import Settings

pytestmark = pytest.mark.asyncio


class DummyTestRequest(Request):
    @property
    def auth(self):
        return Mock(scopes=["READER"])

    @property
    def user(self):
        return Mock(display_name="username")

    @property
    def app(self):
        return Mock(settings=Settings(jwk_key=None))


@pytest.fixture
def http_request():
    request = DummyTestRequest(
        scope={
            "type": "http",
            "http_version": "1.1",
            "method": "GET",
            "headers": [],
        }
    )
    yield request


async def test_get_temp_access_token(http_request):
    resp = api_router.get_temp_access_token(http_request)
    assert "token" in orjson.loads(resp.body)
