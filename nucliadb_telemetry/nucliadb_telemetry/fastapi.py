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

from typing import Callable, Iterable, List, Optional
from urllib.parse import urlparse

from fastapi import FastAPI
from opentelemetry.instrumentation.asgi import OpenTelemetryMiddleware  # type: ignore
from opentelemetry.instrumentation.fastapi import _get_route_details  # type: ignore
from opentelemetry.trace import Span  # type: ignore

try:
    from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
except ImportError:  # pragma: no cover
    SentryAsgiMiddleware = None  # type: ignore


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
):
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
