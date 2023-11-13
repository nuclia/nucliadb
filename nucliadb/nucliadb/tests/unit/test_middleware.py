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
import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from nucliadb.middleware import ProcessTimeHeaderMiddleware


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
