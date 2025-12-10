# Copyright 2025 Bosutech XXI S.L.
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
#

from unittest.mock import Mock, patch

import pytest
from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from nucliadb_telemetry.fastapi import PrometheusMiddleware, metrics_endpoint
from nucliadb_telemetry.fastapi.tracing import CaptureTraceIdMiddleware


class TestCasePrometheusMiddleware:
    @pytest.fixture(scope="class")
    def app(self):
        app_ = Starlette()
        app_.add_middleware(PrometheusMiddleware)
        app_.add_route("/metrics/", metrics_endpoint)

        @app_.route("/foo/")
        def foo(request):
            return PlainTextResponse("Foo")

        @app_.route("/bar/")
        def bar(request):
            raise ValueError("bar")

        @app_.route("/foo/{bar}/")
        def foobar(request):
            return PlainTextResponse(f"Foo: {request.path_params['bar']}")

        sub_app = Starlette()

        @sub_app.route("/foobar/")
        def sub_foobar(request):
            return PlainTextResponse("Foobar")

        app_.mount("/sub", sub_app)

        return app_

    @pytest.fixture
    def client(self, app):
        return TestClient(app)

    def test_view_ok(self, client):
        # Do a request
        client.get("/foo/")

        # Get metrics
        response = client.get("/metrics/")
        metrics_text = response.content.decode()

        # Asserts: Requests
        assert 'starlette_requests_total{method="GET",path_template="/foo/"} 1.0' in metrics_text

        # Asserts: Responses
        assert (
            'starlette_responses_total{method="GET",path_template="/foo/",status_code="200"} 1.0'
            in metrics_text
        )

        # Asserts: Requests in progress
        assert 'starlette_requests_in_progress{method="GET",path_template="/foo/"} 0.0' in metrics_text
        assert (
            'starlette_requests_in_progress{method="GET",path_template="/metrics/"} 1.0' in metrics_text
        )

    def test_view_exception(self, client):
        # Do a request
        with pytest.raises(ValueError):
            client.get("/bar/")

        # Get metrics
        response = client.get("/metrics/")
        metrics_text = response.content.decode()

        # Asserts: Requests
        assert 'starlette_requests_total{method="GET",path_template="/bar/"} 1.0' in metrics_text

        # Asserts: Responses
        assert (
            "starlette_exceptions_total{"
            'exception_type="ValueError",method="GET",path_template="/bar/"'
            "} 1.0" in metrics_text
        )
        assert (
            "starlette_responses_total{"
            'method="GET",path_template="/bar/",status_code="500"'
            "} 1.0" in metrics_text
        )

        # Asserts: Requests in progress
        assert 'starlette_requests_in_progress{method="GET",path_template="/bar/"} 0.0' in metrics_text
        assert (
            'starlette_requests_in_progress{method="GET",path_template="/metrics/"} 1.0' in metrics_text
        )

    def test_path_substitution(self, client):
        # Do a request
        client.get("/foo/baz/")

        # Get metrics
        response = client.get("/metrics/")
        metrics_text = response.content.decode()

        # Asserts: Headers
        assert response.headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"

        # Asserts: Requests
        assert 'starlette_requests_total{method="GET",path_template="/foo/{bar}/"} 1.0' in metrics_text

        # Asserts: Responses
        assert (
            'starlette_responses_total{method="GET",path_template="/foo/{bar}/",status_code="200"} 1.0'
            in metrics_text
        )

        # Asserts: Requests in progress
        assert (
            'starlette_requests_in_progress{method="GET",path_template="/foo/{bar}/"} 0.0'
            in metrics_text
        )
        assert (
            'starlette_requests_in_progress{method="GET",path_template="/metrics/"} 1.0' in metrics_text
        )

    def test_sub_path_match(self, client):
        # Do a request
        client.get("/sub/foobar/")

        # Get metrics
        response = client.get("/metrics/")
        metrics_text = response.content.decode()

        # Asserts: Requests
        assert 'starlette_requests_total{method="GET",path_template="/sub/foobar/"} 1.0' in metrics_text


class TestCasePrometheusMiddlewareFilterUnhandledPaths:
    @pytest.fixture(scope="class")
    def app(self):
        app_ = Starlette()
        app_.add_middleware(PrometheusMiddleware)
        app_.add_route("/metrics/", metrics_endpoint)

        return app_

    @pytest.fixture
    def client(self, app):
        return TestClient(app)

    def test_filter_unhandled_paths(self, client):
        # Do a request
        path = "/other/unhandled/path"
        client.get(path)

        # Get metrics
        response = client.get("/metrics/")
        metrics_text = response.content.decode()

        # Asserts: metric is filtered
        assert path not in metrics_text

        # Asserts: Requests in progress
        assert (
            'starlette_requests_in_progress{method="GET",path_template="/metrics/"} 1.0' in metrics_text
        )


class TestCaseCaptureTraceIdMiddleware:
    @pytest.fixture(scope="class")
    def app(self):
        app_ = Starlette()
        app_.add_middleware(CaptureTraceIdMiddleware)

        @app_.route("/foo/")
        def foo(request):
            return PlainTextResponse("Foo")

        return app_

    @pytest.fixture
    def client(self, app):
        return TestClient(app)

    def test_trace_id_header_is_returned(self, client):
        # if tracing not configured, we don't return it
        response = client.get("/foo/")
        assert "x-nuclia-trace-id" not in response.headers

        # otherwise, we return the header
        span = Mock()
        span.get_span_context.return_value = Mock(trace_id=10)
        with patch("nucliadb_telemetry.fastapi.tracing.trace.get_current_span", return_value=span):
            response = client.get("/foo/")
            assert response.headers["x-nuclia-trace-id"] == "0000000000000000000000000000000a"
