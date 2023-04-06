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

import time
from typing import Callable, Iterable, List, Optional, Tuple
from urllib.parse import urlparse

import prometheus_client  # type: ignore
from fastapi import FastAPI
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware  # type: ignore
from opentelemetry.instrumentation.fastapi import _get_route_details  # type: ignore
from opentelemetry.trace import Span  # type: ignore
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Match, Mount, Route
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR
from starlette.types import ASGIApp, Scope

try:
    from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
except ImportError:  # pragma: no cover
    SentryAsgiMiddleware = None  # type: ignore


async def metrics_endpoint(request):
    output = prometheus_client.exposition.generate_latest()
    return PlainTextResponse(
        output.decode("utf8"), headers={"Content-Type": CONTENT_TYPE_LATEST}
    )


application_metrics = FastAPI(title="Metrics")  # type: ignore
application_metrics.add_route("/metrics", metrics_endpoint)


try:
    from starlette_prometheus.middleware import (
        EXCEPTIONS,
        REQUESTS,
        REQUESTS_IN_PROGRESS,
        REQUESTS_PROCESSING_TIME,
        RESPONSES,
    )
except ImportError:  # pragma: no cover
    # prometheus does not allow duplicate metric names, so we need to
    # conditionally import to avoid conflicts when both are installed
    REQUESTS = Counter(
        "starlette_requests_total",
        "Total count of requests by method and path.",
        ["method", "path_template"],
    )
    RESPONSES = Counter(
        "starlette_responses_total",
        "Total count of responses by method, path and status codes.",
        ["method", "path_template", "status_code"],
    )
    REQUESTS_PROCESSING_TIME = Histogram(
        "starlette_requests_processing_time_seconds",
        "Histogram of requests processing time by path (in seconds)",
        ["method", "path_template"],
    )
    EXCEPTIONS = Counter(
        "starlette_exceptions_total",
        "Total count of exceptions raised by path and exception type",
        ["method", "path_template", "exception_type"],
    )
    REQUESTS_IN_PROGRESS = Gauge(
        "starlette_requests_in_progress",
        "Gauge of requests by method and path currently being processed",
        ["method", "path_template"],
    )


class PrometheusMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, filter_unhandled_paths: bool = False) -> None:
        super().__init__(app)
        self.filter_unhandled_paths = filter_unhandled_paths

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        method = request.method
        path_template, is_handled_path = self.get_path_template(request)

        if self._is_path_filtered(is_handled_path):
            return await call_next(request)

        REQUESTS_IN_PROGRESS.labels(method=method, path_template=path_template).inc()
        REQUESTS.labels(method=method, path_template=path_template).inc()
        before_time = time.perf_counter()
        try:
            response = await call_next(request)
        except BaseException as e:
            status_code = HTTP_500_INTERNAL_SERVER_ERROR
            EXCEPTIONS.labels(
                method=method,
                path_template=path_template,
                exception_type=type(e).__name__,
            ).inc()
            raise e from None
        else:
            status_code = response.status_code
            after_time = time.perf_counter()
            REQUESTS_PROCESSING_TIME.labels(
                method=method, path_template=path_template
            ).observe(after_time - before_time)
        finally:
            RESPONSES.labels(
                method=method, path_template=path_template, status_code=status_code
            ).inc()
            REQUESTS_IN_PROGRESS.labels(
                method=method, path_template=path_template
            ).dec()

        return response

    @staticmethod
    def get_path_template(request: Request) -> Tuple[str, bool]:
        path, found = PrometheusMiddleware._find_route(
            request.scope, request.app.routes
        )
        return path or request.url.path, found

    @staticmethod
    def _find_route(scope: Scope, routes: list[Route]) -> Tuple[Optional[str], bool]:
        # we mutate scope, so we need a copy
        scope = scope.copy()  # type:ignore
        for route in routes:
            if isinstance(route, Mount):
                mount_match, child_scope = route.matches(scope)
                if mount_match == Match.FULL:
                    scope.update(child_scope)
                    sub_path, sub_match = PrometheusMiddleware._find_route(
                        scope, route.routes
                    )
                    if sub_match:
                        return route.path + sub_path, sub_match
            elif isinstance(route, Route):
                match, child_scope = route.matches(scope)
                if match == Match.FULL:
                    return route.path, True
        return None, False

    def _is_path_filtered(self, is_handled_path: bool) -> bool:
        return self.filter_unhandled_paths and not is_handled_path


class ExcludeList:

    """Class to exclude certain paths (given as a list of regexes) from tracing requests"""

    def __init__(self, excluded_urls: Iterable[str]):
        self._excluded_urls = excluded_urls

    def url_disabled(self, url: str) -> bool:
        return bool(self._excluded_urls and urlparse(url).path in self._excluded_urls)


_ServerRequestHookT = Optional[Callable[[Span, dict], None]]
_ClientRequestHookT = Optional[Callable[[Span, dict], None]]
_ClientResponseHookT = Optional[Callable[[Span, dict], None]]


def instrument_app(
    app: FastAPI,
    excluded_urls: List[str],
    server_request_hook: _ServerRequestHookT = None,
    client_request_hook: _ClientRequestHookT = None,
    client_response_hook: _ClientResponseHookT = None,
    tracer_provider=None,
    metrics=False,
):
    if metrics:
        # b/w compat
        app.add_middleware(PrometheusMiddleware)
    if SentryAsgiMiddleware is not None:
        app.add_middleware(SentryAsgiMiddleware)

    excluded_urls_obj = ExcludeList(excluded_urls)

    app.add_middleware(
        OpenTelemetryMiddleware,
        excluded_urls=excluded_urls_obj,
        default_span_details=_get_route_details,
        server_request_hook=server_request_hook,
        client_request_hook=client_request_hook,
        client_response_hook=client_response_hook,
        tracer_provider=tracer_provider,
    )
