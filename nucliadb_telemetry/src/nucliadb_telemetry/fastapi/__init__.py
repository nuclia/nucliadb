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

from typing import Iterable, List
from urllib.parse import urlparse

import prometheus_client
from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import (  # type: ignore
    _get_default_span_details,
)
from prometheus_client import CONTENT_TYPE_LATEST
from starlette.responses import PlainTextResponse

from nucliadb_telemetry import errors
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
    return PlainTextResponse(output.decode("utf8"), headers={"Content-Type": CONTENT_TYPE_LATEST})


application_metrics = FastAPI(title="Metrics")
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
    if tracer_provider is not None:
        app.add_middleware(
            OpenTelemetryMiddleware,
            excluded_urls=excluded_urls_obj,
            default_span_details=_get_default_span_details,
            server_request_hook=server_request_hook,
            tracer_provider=tracer_provider,
        )

    error_settings = errors.ErrorHandlingSettings()
    if SentryAsgiMiddleware is not None and error_settings.sentry_url is not None:
        # add last to catch all exceptions
        # `add_middleware` always adds to the beginning of the middleware list
        app.add_middleware(SentryAsgiMiddleware)
