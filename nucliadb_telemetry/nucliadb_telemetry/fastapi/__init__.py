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

from typing import Iterable, List
from urllib.parse import urlparse

import prometheus_client  # type: ignore
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import (  # type: ignore
    _get_default_span_details,
)
from prometheus_client import CONTENT_TYPE_LATEST
from starlette.responses import PlainTextResponse

from nucliadb_telemetry.fastapi.metrics import PrometheusMiddleware
from nucliadb_telemetry.fastapi.tracing import (
    CaptureTraceIdMiddleware,
    OpenTelemetryMiddleware,
    ServerRequestHookT,
)

from .context import ContextInjectorMiddleware

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


class ExcludeList:

    """Class to exclude certain paths (given as a list of regexes) from tracing requests"""

    def __init__(self, excluded_urls: Iterable[str]):
        self._excluded_urls = excluded_urls

    def url_disabled(self, url: str) -> bool:
        return bool(self._excluded_urls and urlparse(url).path in self._excluded_urls)


def instrument_app(
    app: FastAPI,
    excluded_urls: List[str],
    server_request_hook: ServerRequestHookT = None,
    tracer_provider=None,
    metrics=False,
    trace_id_on_responses: bool = False,
):
    """
    :param trace_id_on_responses: If set to True, trace ids will be returned in the
                                  X-NUCLIA-TRACE-ID header for all HTTP responses of
                                  this app.
    """
    if metrics:
        # b/w compat
        app.add_middleware(PrometheusMiddleware)

    excluded_urls_obj = ExcludeList(excluded_urls)

    if trace_id_on_responses:
        # Trace ids are provided by OpenTelemetryMiddleware,
        # so the capture middleware needs to be added before.
        app.add_middleware(CaptureTraceIdMiddleware)

    app.add_middleware(ContextInjectorMiddleware)
    app.add_middleware(
        OpenTelemetryMiddleware,
        excluded_urls=excluded_urls_obj,
        default_span_details=_get_default_span_details,
        server_request_hook=server_request_hook,
        tracer_provider=tracer_provider,
    )

    if SentryAsgiMiddleware is not None:
        # add last to catch all exceptions
        # `add_middleware` always adds to the beginning of the middleware list
        app.add_middleware(SentryAsgiMiddleware)
