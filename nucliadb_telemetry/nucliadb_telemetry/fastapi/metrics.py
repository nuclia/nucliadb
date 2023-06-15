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

from prometheus_client import Counter, Gauge, Histogram
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.status import HTTP_500_INTERNAL_SERVER_ERROR

from .utils import get_path_template

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
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
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
