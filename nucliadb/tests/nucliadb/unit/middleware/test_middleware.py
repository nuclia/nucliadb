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
from fastapi import HTTPException
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from nucliadb.middleware import (
    ClientErrorPayloadLoggerMiddleware,
    EventCounter,
    ProcessTimeHeaderMiddleware,
)


class TestCaseProcessTimeHeaderMiddleware:
    @pytest.fixture(scope="class")
    def app(self):
        app_ = Starlette()
        app_.add_middleware(ProcessTimeHeaderMiddleware)

        @app_.route("/foo/")
        def foo(request):
            return PlainTextResponse("Foo")

        return app_

    @pytest.fixture
    def client(self, app):
        return TestClient(app)

    def test_process_time_header_is_returned(self, client):
        response = client.get("/foo/")

        assert response.headers["access-control-expose-headers"] == "X-PROCESS-TIME"
        assert float(response.headers["x-process-time"]) > 0


@pytest.fixture(scope="function")
def app():
    ClientErrorPayloadLoggerMiddleware.ip_counters.clear()

    app_ = Starlette()
    app_.add_middleware(ClientErrorPayloadLoggerMiddleware)

    @app_.route("/foo/")
    def foo(request):
        raise HTTPException(status_code=412, detail="Precondition Failed")

    yield app_

    ClientErrorPayloadLoggerMiddleware.ip_counters.clear()


def test_client_error_payload_is_logged(app, caplog):
    caplog.clear()
    client = TestClient(app)
    with caplog.at_level("INFO"):
        response = client.get("/foo/")

        assert response.status_code == 412
        assert "Client error. Response payload: Precondition Failed" in caplog.text

        for _ in range(ClientErrorPayloadLoggerMiddleware.max_events_per_ip + 10):
            response = client.get("/foo/")

        log_count = caplog.text.count("Client error. Response payload: Precondition Failed")
        assert log_count == ClientErrorPayloadLoggerMiddleware.max_events_per_ip


def test_event_counter():
    counter = EventCounter(window_seconds=2)

    for _ in range(200):
        counter.log_event()

    assert counter.get_count() == 200
    time.sleep(2.1)
    assert counter.get_count() == 0
