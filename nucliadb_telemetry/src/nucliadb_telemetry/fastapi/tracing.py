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
import typing
import urllib
from functools import wraps
from typing import Callable, Optional, Tuple

from asgiref.compatibility import guarantee_single_callable
from fastapi import Request, Response
from opentelemetry import context, trace
from opentelemetry.instrumentation.asgi.version import __version__  # noqa
from opentelemetry.instrumentation.propagators import get_global_response_propagator
from opentelemetry.instrumentation.utils import (
    _start_internal_or_server_span,
    http_status_to_status_code,
)
from opentelemetry.propagators.textmap import Getter, Setter
from opentelemetry.semconv.trace import SpanAttributes
from opentelemetry.trace import Span, format_trace_id, set_span_in_context
from opentelemetry.trace.status import Status, StatusCode
from opentelemetry.util.http import (
    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS,
    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST,
    OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE,
    SanitizeValue,
    get_custom_headers,
    normalise_request_header_name,
    normalise_response_header_name,
    remove_url_credentials,
)
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

ServerRequestHookT = Optional[Callable[[Span, dict], None]]


NUCLIA_TRACE_ID_HEADER = "X-NUCLIA-TRACE-ID"
ACCESS_CONTROL_EXPOSE_HEADER = "Access-Control-Expose-Headers"

# ----------------------------
# Forked from https://raw.githubusercontent.com/open-telemetry/opentelemetry-python-contrib/main/instrumentation/opentelemetry-instrumentation-asgi/src/opentelemetry/instrumentation/asgi/__init__.py
#
# Changes:
#
# - Remove receive and send hooks
# - Remove adding spans on receive
# - Remove adding spans on send, while keeping the header extraction and propagation
# ----------------------------


class ASGIGetter(Getter[dict]):
    def get(self, carrier: dict, key: str) -> typing.Optional[typing.List[str]]:
        """Getter implementation to retrieve a HTTP header value from the ASGI
        scope.

        Args:
            carrier: ASGI scope object
            key: header name in scope
        Returns:
            A list with a single string with the header value if it exists,
                else None.
        """
        headers = carrier.get("headers")
        if not headers:
            return None

        # ASGI header keys are in lower case
        key = key.lower()
        decoded = [
            _value.decode("utf8", errors="replace")
            for (_key, _value) in headers
            if _key.decode("utf8", errors="replace").lower() == key
        ]
        if not decoded:
            return None
        return decoded

    def keys(self, carrier: dict) -> typing.List[str]:
        headers = carrier.get("headers") or []
        return [_key.decode("utf8", errors="replace") for (_key, _) in headers]


asgi_getter = ASGIGetter()


class ASGISetter(Setter[dict]):
    def set(self, carrier: dict, key: str, value: str) -> None:  # pylint: disable=no-self-use
        """Sets response header values on an ASGI scope according to `the spec <https://asgi.readthedocs.io/en/latest/specs/www.html#response-start-send-event>`_.

        Args:
            carrier: ASGI scope object
            key: response header name to set
            value: response header value
        Returns:
            None
        """
        headers = carrier.get("headers")
        if not headers:
            headers = []
            carrier["headers"] = headers

        headers.append([key.lower().encode(), value.encode()])


asgi_setter = ASGISetter()


def collect_request_attributes(scope):
    """Collects HTTP request attributes from the ASGI scope and returns a
    dictionary to be used as span creation attributes."""
    server_host, port, http_url = get_host_port_url_tuple(scope)
    query_string = scope.get("query_string")
    if query_string and http_url:
        if isinstance(query_string, bytes):
            query_string = query_string.decode("utf8", errors="replace")
        http_url += "?" + urllib.parse.unquote(query_string)

    result = {
        SpanAttributes.HTTP_SCHEME: scope.get("scheme"),
        SpanAttributes.HTTP_HOST: server_host,
        SpanAttributes.NET_HOST_PORT: port,
        SpanAttributes.HTTP_FLAVOR: scope.get("http_version"),
        SpanAttributes.HTTP_TARGET: scope.get("path"),
        SpanAttributes.HTTP_URL: remove_url_credentials(http_url),
    }
    http_method = scope.get("method")
    if http_method:
        result[SpanAttributes.HTTP_METHOD] = http_method

    http_host_value_list = asgi_getter.get(scope, "host")
    if http_host_value_list:
        result[SpanAttributes.HTTP_SERVER_NAME] = ",".join(http_host_value_list)
    http_user_agent = asgi_getter.get(scope, "user-agent")
    if http_user_agent:
        result[SpanAttributes.HTTP_USER_AGENT] = http_user_agent[0]

    if "client" in scope and scope["client"] is not None:
        result[SpanAttributes.NET_PEER_IP] = scope.get("client")[0]
        result[SpanAttributes.NET_PEER_PORT] = scope.get("client")[1]

    # remove None values
    result = {k: v for k, v in result.items() if v is not None}

    return result


def collect_custom_request_headers_attributes(scope):
    """returns custom HTTP request headers to be added into SERVER span as span attributes
    Refer specification https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#http-request-and-response-headers
    """

    sanitize = SanitizeValue(
        get_custom_headers(OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS)
    )

    # Decode headers before processing.
    headers = {
        _key.decode("utf8", errors="replace"): _value.decode("utf8", errors="replace")
        for (_key, _value) in scope.get("headers")
    }

    return sanitize.sanitize_header_values(
        headers,
        get_custom_headers(OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_REQUEST),
        normalise_request_header_name,
    )


def collect_custom_response_headers_attributes(message):
    """returns custom HTTP response headers to be added into SERVER span as span attributes
    Refer specification https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/http.md#http-request-and-response-headers
    """

    sanitize = SanitizeValue(
        get_custom_headers(OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SANITIZE_FIELDS)
    )

    # Decode headers before processing.
    headers = {
        _key.decode("utf8", errors="replace"): _value.decode("utf8", errors="replace")
        for (_key, _value) in message.get("headers")
    }

    return sanitize.sanitize_header_values(
        headers,
        get_custom_headers(OTEL_INSTRUMENTATION_HTTP_CAPTURE_HEADERS_SERVER_RESPONSE),
        normalise_response_header_name,
    )


def get_host_port_url_tuple(scope):
    """Returns (host, port, full_url) tuple."""
    server = scope.get("server") or ["0.0.0.0", 80]
    port = server[1]
    server_host = server[0] + (":" + str(port) if str(port) != "80" else "")
    full_path = scope.get("root_path", "") + scope.get("path", "")
    http_url = scope.get("scheme", "http") + "://" + server_host + full_path
    return server_host, port, http_url


def set_status_code(span, status_code):
    """Adds HTTP response attributes to span using the status_code argument."""
    if not span.is_recording():
        return
    try:
        status_code = int(status_code)
    except ValueError:
        span.set_status(
            Status(
                StatusCode.ERROR,
                "Non-integer HTTP status: " + repr(status_code),
            )
        )
    else:
        span.set_attribute(SpanAttributes.HTTP_STATUS_CODE, status_code)
        span.set_status(Status(http_status_to_status_code(status_code, server_span=True)))


def get_default_span_details(scope: dict) -> Tuple[str, dict]:
    """
    Default span name is the HTTP method and URL path, or just the method.
    https://github.com/open-telemetry/opentelemetry-specification/pull/3165
    https://opentelemetry.io/docs/reference/specification/trace/semantic_conventions/http/#name

    Args:
        scope: the ASGI scope dictionary
    Returns:
        a tuple of the span name, and any attributes to attach to the span.
    """
    path = scope.get("path", "").strip()
    method = scope.get("method", "").strip()
    if method and path:  # http
        return f"{method} {path}", {}
    if path:  # websocket
        return path, {}
    return method, {}  # http with no path


class OpenTelemetryMiddleware:
    """The ASGI application middleware.
    This class is an ASGI middleware that starts and annotates spans for any
    requests it is invoked with.
    Args:
        app: The ASGI application callable to forward requests to.
        default_span_details: Callback which should return a string and a tuple, representing the desired default
                      span name and a dictionary with any additional span attributes to set.
                      Optional: Defaults to get_default_span_details.
        server_request_hook: Optional callback which is called with the server span and ASGI
                      scope object for every incoming request.
        tracer_provider: The optional tracer provider to use. If omitted
            the current globally configured one is used.
    """

    def __init__(
        self,
        app,
        excluded_urls=None,
        default_span_details=None,
        server_request_hook: ServerRequestHookT = None,
        tracer_provider=None,
    ):
        self.app = guarantee_single_callable(app)
        self.tracer = trace.get_tracer(__name__, __version__, tracer_provider)
        self.excluded_urls = excluded_urls
        self.default_span_details = default_span_details or get_default_span_details
        self.server_request_hook = server_request_hook

    async def __call__(self, scope, receive, send):
        """The ASGI application

        Args:
            scope: An ASGI environment.
            receive: An awaitable callable yielding dictionaries
            send: An awaitable callable taking a single dictionary as argument.
        """
        if scope["type"] not in ("http", "websocket"):
            return await self.app(scope, receive, send)

        _, _, url = get_host_port_url_tuple(scope)
        if self.excluded_urls and self.excluded_urls.url_disabled(url):
            return await self.app(scope, receive, send)

        span_name, additional_attributes = self.default_span_details(scope)

        span, token = _start_internal_or_server_span(
            tracer=self.tracer,
            span_name=span_name,
            start_time=None,
            context_carrier=scope,
            context_getter=asgi_getter,
        )

        try:
            with trace.use_span(span, end_on_exit=False) as current_span:
                if current_span.is_recording():
                    attributes = collect_request_attributes(scope)
                    attributes.update(additional_attributes)

                    for key, value in attributes.items():
                        current_span.set_attribute(key, value)

                    if current_span.kind == trace.SpanKind.SERVER:
                        custom_attributes = collect_custom_request_headers_attributes(scope)
                        if len(custom_attributes) > 0:
                            current_span.set_attributes(custom_attributes)

                if callable(self.server_request_hook):
                    self.server_request_hook(current_span, scope)

                otel_send = self._get_otel_send(current_span, span_name, scope, send)

                await self.app(scope, receive, otel_send)
        finally:
            if token:
                context.detach(token)
            if span.is_recording():
                span.end()

    def _get_otel_send(self, server_span, server_span_name, scope, send):
        @wraps(send)
        async def otel_send(message):
            if message["type"] == "http.response.start":
                status_code = message["status"]
                set_status_code(server_span, status_code)
            elif message["type"] == "websocket.send":
                set_status_code(server_span, 200)
            if (
                server_span.is_recording()
                and server_span.kind == trace.SpanKind.SERVER
                and "headers" in message
            ):
                custom_response_attributes = collect_custom_response_headers_attributes(message)
                if len(custom_response_attributes) > 0:
                    server_span.set_attributes(custom_response_attributes)

            propagator = get_global_response_propagator()
            if propagator:
                propagator.inject(
                    message,
                    context=set_span_in_context(server_span, trace.context_api.Context()),
                    setter=asgi_setter,
                )

            await send(message)

        return otel_send


class CaptureTraceIdMiddleware(BaseHTTPMiddleware):
    def capture_trace_id(self, response):
        span = trace.get_current_span()
        if span is None:
            return
        trace_id = format_trace_id(span.get_span_context().trace_id)
        response.headers[NUCLIA_TRACE_ID_HEADER] = trace_id

    def expose_trace_id_header(self, response):
        exposed_headers = []
        if ACCESS_CONTROL_EXPOSE_HEADER in response.headers:
            exposed_headers = response.headers[ACCESS_CONTROL_EXPOSE_HEADER].split(",")
        if NUCLIA_TRACE_ID_HEADER not in exposed_headers:
            exposed_headers.append(NUCLIA_TRACE_ID_HEADER)
            response.headers[ACCESS_CONTROL_EXPOSE_HEADER] = ",".join(exposed_headers)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = None
        try:
            response = await call_next(request)
        finally:
            if response is not None:
                self.capture_trace_id(response)
                self.expose_trace_id_header(response)
                return response
