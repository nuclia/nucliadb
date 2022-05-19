from typing import Callable, List, Optional

from fastapi import FastAPI
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware  # type: ignore
from opentelemetry.instrumentation.fastapi import _get_route_details  # type: ignore
from opentelemetry.trace import Span  # type: ignore

from nucliadb_utils.fastapi.exclude_urls import ExcludeList

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
):
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
