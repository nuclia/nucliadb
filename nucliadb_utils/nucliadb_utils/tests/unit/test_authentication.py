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

    @pytest.mark.asyncio
    async def test_authenticate(
        self, backend: authentication.NucliaCloudAuthenticationBackend, req
    ):
        assert await backend.authenticate(req) is None

    @pytest.mark.asyncio
    async def test_authenticate_with_user(
        self, backend: authentication.NucliaCloudAuthenticationBackend, req
    ):
        req.headers = {
            backend.roles_header: "admin",
            backend.user_header: "user",
        }

        creds, user = await backend.authenticate(req)

        assert creds.scopes == ["admin"]
        assert user.username == "user"

    @pytest.mark.asyncio
    async def test_authenticate_with_anon(
        self, backend: authentication.NucliaCloudAuthenticationBackend, req
    ):
        req.headers = {backend.roles_header: "admin"}

        creds, user = await backend.authenticate(req)

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

    def test_requires_sync_returns_status(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        with pytest.raises(HTTPException):
            assert authentication.requires(["foobar"])(lambda request: None)(req)

    def test_requires_sync_returns_redirect(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        with patch.object(req, "url_for", return_value="http://foobar"):
            resp = authentication.requires(["foobar"], redirect="/foobar")(
                lambda request: None
            )(req)
        assert resp.status_code == 303

    @pytest.mark.asyncio
    async def test_requires_async(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        async def noop(request):
            ...

        assert await authentication.requires(["admin"])(noop)(request=req) is None

    @pytest.mark.asyncio
    async def test_requires_async_returns_status(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        async def noop(request):
            ...

        with pytest.raises(HTTPException):
            assert await authentication.requires(["foobar"])(noop)(request=req)

    @pytest.mark.asyncio
    async def test_requires_async_returns_redirect(self):
        req = Request({"type": "http", "auth": Mock(scopes=["admin"])})

        async def noop(request):
            ...

        with patch.object(req, "url_for", return_value="http://foobar"):
            resp = await authentication.requires(["foobar"], redirect="/foobar")(noop)(
                request=req
            )
        assert resp.status_code == 303

    @pytest.mark.asyncio
    async def test_requires_ws(self):
        ws = AsyncMock()
        req = WebSocket(
            {"type": "websocket", "auth": Mock(scopes=["admin"]), "websocket": ws},
            receive=AsyncMock(),
            send=AsyncMock(),
        )

        async def noop(websocket):
            ...

        assert await authentication.requires(["admin"])(noop)(req) is None

    @pytest.mark.asyncio
    async def test_requires_ws_fail(self):
        req = WebSocket(
            {"type": "websocket", "auth": Mock(scopes=["admin"])},
            receive=AsyncMock(),
            send=AsyncMock(),
        )

        async def noop(websocket):
            ...

        with patch.object(req, "close", return_value=None):
            assert await authentication.requires(["notallowed"])(noop)(req) is None

            req.close.assert_called_once()
