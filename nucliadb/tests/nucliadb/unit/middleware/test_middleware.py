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

import pytest
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from nucliadb.middleware import (
    EventCounter,
    ProcessTimeHeaderMiddleware,
    UUIDPathParamsValidationMiddleware,
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


class TestCaseUUIDPathParamsValidationMiddleware:
    VALID_KBID = "d6fcb6d1-4525-4a1d-84a5-35c7be5dbfbb"
    VALID_RID = "d6fcb6d145254a1d84a535c7be5dbfbb"

    @pytest.fixture(scope="class")
    def app(self):
        calls = {"ok": 0}

        def ok(request):
            calls["ok"] += 1
            return PlainTextResponse("ok")

        app = Starlette(
            routes=[
                Route("/kb/{kbid}/resource/{rid}", ok),
                Route("/kb/{kbid}/resource/{path_rid}/reprocess", ok),
                Route("/status", ok),
            ],
            middleware=[
                Middleware(UUIDPathParamsValidationMiddleware),
            ],
        )
        app.state.calls = calls
        yield app

    @pytest.fixture
    def client(self, app):
        return TestClient(app)

    def test_invalid_kbid_returns_404(self, client):
        initial_calls = client.app.state.calls["ok"]
        response = client.get(f"/kb/not-a-uuid/resource/{self.VALID_RID}")

        assert response.status_code == 404
        assert response.json()["detail"] == "KnowledgeBox not found. UUID expected."
        assert client.app.state.calls["ok"] == initial_calls

    def test_invalid_rid_returns_404(self, client):
        response = client.get(f"/kb/{self.VALID_KBID}/resource/not-a-uuid")

        assert response.status_code == 404
        assert response.json()["detail"] == "Resource not found. UUID expected."

    def test_invalid_path_rid_returns_404(self, client):
        response = client.get(f"/kb/{self.VALID_KBID}/resource/not-a-uuid/reprocess")

        assert response.status_code == 404
        assert response.json()["detail"] == "Resource not found. UUID expected."

    def test_valid_ids_are_accepted(self, client):
        response = client.get(f"/kb/{self.VALID_KBID}/resource/{self.VALID_RID}")

        assert response.status_code == 200
        assert response.text == "ok"

    def test_non_kb_paths_bypass_validation(self, client):
        response = client.get("/status")

        assert response.status_code == 200
        assert response.text == "ok"
