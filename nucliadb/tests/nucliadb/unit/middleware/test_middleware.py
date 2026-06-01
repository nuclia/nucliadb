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

import time
from unittest.mock import AsyncMock, patch

import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from nucliadb.middleware import (
    _KB_UUID_PATH_RE,
    EventCounter,
    KBExistsMiddleware,
    ProcessTimeHeaderMiddleware,
)


class TestCaseProcessTimeHeaderMiddleware:
    @pytest.fixture(scope="class")
    def app(self):
        def foo(request):
            return PlainTextResponse("Foo")

        app = Starlette(
            routes=[
                Route("/foo/", foo),
            ],
            middleware=[
                Middleware(ProcessTimeHeaderMiddleware),
            ],
        )
        yield app

    @pytest.fixture
    def client(self, app):
        return TestClient(app)

    def test_process_time_header_is_returned(self, client):
        response = client.get("/foo/")

        assert response.headers["access-control-expose-headers"] == "X-PROCESS-TIME"
        assert float(response.headers["x-process-time"]) > 0


def test_event_counter():
    counter = EventCounter(window_seconds=2)

    for _ in range(200):
        counter.log_event()

    assert counter.get_count() == 200
    time.sleep(2.1)
    assert counter.get_count() == 0


KBID = "4b9a1e20-df3c-4e97-a9b6-1c2d3e4f5a6b"

# (path, should_match)
KB_UUID_PATH_CASES = [
    # --- writer resource endpoints ---
    (f"/api/v1/kb/{KBID}/resources", True),
    (f"/api/v1/kb/{KBID}/resource/somerid", True),
    (f"/api/v1/kb/{KBID}/slug/my-slug", True),
    (f"/api/v1/kb/{KBID}/resource/somerid/reprocess", True),
    (f"/api/v1/kb/{KBID}/resource/somerid/reindex", True),
    (f"/api/v1/kb/{KBID}/upload", True),
    (f"/api/v1/kb/{KBID}/tusupload/some-upload-id", True),
    # --- writer service endpoints ---
    (f"/api/v1/kb/{KBID}/labelset/my-labelset", True),
    (f"/api/v1/kb/{KBID}/export", True),
    (f"/api/v1/kb/{KBID}/configuration", True),
    # --- path without a kbid (no match expected) ---
    ("/api/v1/kbs", False),
    ("/api/v1/kb/not-a-uuid/resources", False),
    ("/api/v1/kb/my-slug/resources", False),
    # --- bare /kb/<uuid> with no trailing slash/segment ---
    (f"/api/v1/kb/{KBID}", True),
]


@pytest.mark.parametrize("path,should_match", KB_UUID_PATH_CASES)
def test_kb_uuid_path_regex(path, should_match):
    match = _KB_UUID_PATH_RE.search(path)
    if should_match:
        assert match is not None, f"Expected match for path: {path}"
        assert match.group(1).lower() == KBID.lower()
    else:
        assert match is None, f"Expected no match for path: {path}"


class TestKBExistsMiddleware:
    @pytest.fixture()
    def app(self):
        def endpoint(request):
            return PlainTextResponse("ok")

        return Starlette(
            routes=[
                Route(f"/api/v1/kb/{{kbid}}/resources", endpoint),
                Route("/api/v1/kbs", endpoint),
            ],
            middleware=[Middleware(KBExistsMiddleware)],
        )

    @pytest.fixture()
    def client(self, app):
        return TestClient(app, raise_server_exceptions=False)

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        # Clear the _kb_exists_cache before each test to avoid interference between tests
        from nucliadb.middleware import _kb_exists_cache

        _kb_exists_cache.clear()

    def test_kb_exists_passes_through(self, client):
        with patch(
            "nucliadb.middleware.datamanagers.atomic.kb.exists_kb",
            new=AsyncMock(return_value=True),
        ):
            response = client.get(f"/api/v1/kb/{KBID}/resources")
        assert response.status_code == 200

    def test_kb_not_found_returns_404(self, client):
        with patch(
            "nucliadb.middleware.datamanagers.atomic.kb.exists_kb",
            new=AsyncMock(return_value=False),
        ):
            response = client.get(f"/api/v1/kb/{KBID}/resources")
        assert response.status_code == 404
        assert KBID in response.json()["detail"]

    def test_non_uuid_path_skips_check(self, client):
        with patch(
            "nucliadb.middleware.datamanagers.atomic.kb.exists_kb",
            new=AsyncMock(side_effect=AssertionError("should not be called")),
        ):
            # /kbs has no kbid in path — middleware must not call exists_kb
            response = client.get("/api/v1/kbs")
        assert response.status_code == 200

    def test_slug_path_skips_check(self, client):
        with patch(
            "nucliadb.middleware.datamanagers.atomic.kb.exists_kb",
            new=AsyncMock(side_effect=AssertionError("should not be called")),
        ):
            client.get("/api/v1/kb/my-slug/resources")
