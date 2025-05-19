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

import time

from prometheus_client import Counter, Gauge, Histogram
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from .utils import get_path_template

try:
    from starlette_prometheus.middleware import (  # type: ignore
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
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        method = request.method
        found_path_template = get_path_template(request.scope)

        if not found_path_template.match:
            return await call_next(request)

        path_template = found_path_template.path

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
            REQUESTS_PROCESSING_TIME.labels(method=method, path_template=path_template).observe(
                after_time - before_time
            )
        finally:
            RESPONSES.labels(method=method, path_template=path_template, status_code=status_code).inc()
            REQUESTS_IN_PROGRESS.labels(method=method, path_template=path_template).dec()

        return response
