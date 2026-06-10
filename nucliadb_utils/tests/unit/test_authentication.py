# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi import WebSocket
from starlette.exceptions import HTTPException
from starlette.requests import Request

from nucliadb_utils import authentication


class TestNucliaCloudAuthenticationBackend:
    @pytest.fixture()
    def backend(self):
        return authentication.NucliaCloudAuthenticationBackend()

    @pytest.fixture()
    def req(self):
        return Mock(headers={})

    async def test_authenticate(self, backend: authentication.NucliaCloudAuthenticationBackend, req):
        assert await backend.authenticate(req) is None

    async def test_authenticate_with_user(
        self, backend: authentication.NucliaCloudAuthenticationBackend, req
    ):
        req.headers = {
            backend.roles_header: "admin",
            backend.user_header: "user",
        }

        auth_result = await backend.authenticate(req)
        assert auth_result
        creds, user = auth_result

        assert creds.scopes == ["admin"]
        assert user.username == "user"

    async def test_authenticate_with_anon(
        self, backend: authentication.NucliaCloudAuthenticationBackend, req
    ):
        req.headers = {backend.roles_header: "admin"}

        auth_result = await backend.authenticate(req)
        assert auth_result
        creds, user = auth_result

        assert creds.scopes == ["admin"]
        assert user.username == "anonymous"


def test_has_required_scope():
    conn = Mock(auth=Mock(scopes=["admin"]))
    assert authentication.has_required_scope(conn, ["admin"])
    assert not authentication.has_required_scope(conn, ["foobar"])


class TestRequires:
    def test_requires_sync(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        assert authentication.requires(["admin"])(lambda request: None)(req) is None

        # test passed as kwargs
        assert authentication.requires(["admin"])(lambda request: None)(request=req) is None

    def test_requires_sync_returns_status(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        with pytest.raises(HTTPException):
            assert authentication.requires(["foobar"])(lambda request: None)(req)

    def test_requires_sync_returns_redirect(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        with patch.object(req, "url_for", return_value="http://foobar"):
            resp = authentication.requires(["foobar"], redirect="/foobar")(lambda request: None)(req)
        assert resp.status_code == 303

    async def test_requires_async(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        async def noop(request): ...

        assert await authentication.requires(["admin"])(noop)(request=req) is None

    async def test_requires_async_returns_status(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        async def noop(request): ...

        with pytest.raises(HTTPException):
            assert await authentication.requires(["foobar"])(noop)(request=req)

    async def test_requires_async_returns_redirect(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        async def noop(request): ...

        with patch.object(req, "url_for", return_value="http://foobar"):
            resp = await authentication.requires(["foobar"], redirect="/foobar")(noop)(request=req)
        assert resp.status_code == 303

    async def test_requires_ws(self):
        ws = AsyncMock()
        req = WebSocket(
            {"type": "websocket", "auth": Mock(scopes=["admin"]), "websocket": ws},
            receive=AsyncMock(),
            send=AsyncMock(),
        )

        async def noop(websocket): ...

        assert await authentication.requires(["admin"])(noop)(req) is None

    async def test_requires_ws_fail(self):
        req = WebSocket(
            {"type": "websocket", "auth": Mock(scopes=["admin"])},
            receive=AsyncMock(),
            send=AsyncMock(),
        )

        async def noop(websocket): ...

        with patch.object(req, "close", return_value=None):
            assert await authentication.requires(["notallowed"])(noop)(req) is None

            req.close.assert_called_once()  # type: ignore[ty:unresolved-attribute]
