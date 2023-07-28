from unittest.mock import Mock

import orjson
import pytest
from fastapi import Request

from nucliadb.standalone import api_router
from nucliadb.standalone.settings import Settings

pytestmark = pytest.mark.asyncio


class TestRequest(Request):
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
    request = TestRequest(
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
