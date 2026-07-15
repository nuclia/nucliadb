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

import logging
import time
import uuid
from collections import deque
from typing import Callable, ClassVar

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import BaseRoute, Match, Mount
from starlette.types import Scope

PROCESS_TIME_HEADER = "X-PROCESS-TIME"
ACCESS_CONTROL_EXPOSE_HEADER = "Access-Control-Expose-Headers"


logger = logging.getLogger("nucliadb.middleware")


class ProcessTimeHeaderMiddleware(BaseHTTPMiddleware):
    def capture_process_time(self, response, duration: float):
        response.headers[PROCESS_TIME_HEADER] = str(duration)

    def expose_process_time_header(self, response):
        exposed_headers = []
        if ACCESS_CONTROL_EXPOSE_HEADER in response.headers:
            exposed_headers = response.headers[ACCESS_CONTROL_EXPOSE_HEADER].split(",")
        if PROCESS_TIME_HEADER not in exposed_headers:
            exposed_headers.append(PROCESS_TIME_HEADER)
            response.headers[ACCESS_CONTROL_EXPOSE_HEADER] = ",".join(exposed_headers)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = None
        start = time.perf_counter()
        try:
            response = await call_next(request)
            return response
        finally:
            if response is not None:
                duration = time.perf_counter() - start
                self.capture_process_time(response, duration)
                self.expose_process_time_header(response)


class ClientErrorPayloadLoggerMiddleware(BaseHTTPMiddleware):
    """
    Middleware that logs the payload of client error responses (HTTP 412 and 422).
    This helps supporting clients by providing more context about the errors they
    encounter which otherwise we don't have much visibility on.

    There is a limit of logs per IP to avoid flooding the logs in case of
    misbehaving clients.
    """

    log_counters: ClassVar[dict[str, "HourlyLogCounter"]] = {}
    max_logs: int = 200

    def get_request_host(self, request: Request) -> str:
        return request.client.host if request.client else "unknown"

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)

        host = self.get_request_host(request)
        counter = self.log_counters.setdefault(host, HourlyLogCounter())
        if response.status_code in (412, 422) and counter.get_count() < self.max_logs:
            counter.log_event()

            response_body = b""
            chunk: bytes
            async for chunk in response.body_iterator:  # type: ignore
                response_body += chunk

            logger.info(
                f"Client payload validation error",
                extra={
                    "request_method": request.method,
                    "request_path": request.url.path,
                    "response_status_code": response.status_code,
                    "response_payload": response_body.decode("utf-8", errors="replace"),
                },
            )
            # Recreate the response body iterator since it has been consumed
            response = Response(
                content=response_body,
                status_code=response.status_code,
                headers=dict(response.headers),
                media_type=response.media_type,
                background=response.background,
            )
        return response


def valid_kbid(kbid: str) -> bool:
    # Must be a UUID in canonical dashed format.
    if len(kbid) != 36 or kbid.count("-") != 4:
        return False
    try:
        return str(uuid.UUID(kbid)) == kbid.lower()
    except ValueError:
        return False


def valid_rid(rid: str) -> bool:
    # Must be a UUID in hex format.
    if len(rid) != 32 or "-" in rid:
        return False
    try:
        return uuid.UUID(rid).hex == rid.lower()
    except ValueError:
        return False


class UUIDPathParamsValidationMiddleware(BaseHTTPMiddleware):
    """
    Validate UUID-like path params before endpoint execution.

    Why this middleware does route/scope traversal instead of reading
    request.path_params directly:
    - request.path_params is not populated yet at middleware stage.
    - Our standalone app is wrapped with VersionedFastAPI, which mounts
        versioned sub-apps (for example /api/v1). The effective path params
        can be defined inside nested mounted routers.

    This middleware therefore reconstructs path params by matching the
    incoming scope against router routes, recursing into Mount routes when
    needed, and then validating kbid/rid/path_rid.
    """

    _VALIDATORS: ClassVar[dict[str, tuple[Callable[[str], bool], str]]] = {
        "kbid": (valid_kbid, "KnowledgeBox not found. UUID expected."),
        "rid": (valid_rid, "Resource not found. UUID expected."),
        "path_rid": (valid_rid, "Resource not found. UUID expected."),
    }

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Execute before endpoint code: if any tracked param is invalid,
        # return the same 404 contract used by endpoint-level validation.
        path_params = self._resolve_path_params(request)
        for param_name, (validator, error_detail) in self._VALIDATORS.items():
            value = path_params.get(param_name)
            if value is not None and not validator(value):
                return JSONResponse(status_code=404, content={"detail": error_detail})

        return await call_next(request)

    def _resolve_path_params(self, request: Request) -> dict[str, str]:
        # Build a mutable copy of the current scope and normalize path from
        # request URL so matching works consistently through mounted apps.
        scope = dict(request.scope)
        scope["path"] = request.url.path

        router = getattr(request.app, "router", None)
        if router is None:
            return {}

        return self._find_path_params(router.routes, scope) or {}

    def _find_path_params(self, routes: list[BaseRoute], scope: Scope) -> dict[str, str] | None:
        for route in routes:
            match, child_scope = route.matches(scope)
            if match == Match.NONE:
                # This route is not a candidate for the current request scope
                # (for example, different path/method or a sibling route).
                continue

            path_params = dict(child_scope.get("path_params", {}))

            if isinstance(route, Mount):
                route_app = getattr(route, "app", None)
                route_router = getattr(route_app, "router", None)
                if route_router is not None:
                    # This happens for mounted sub-apps (for example the
                    # VersionedFastAPI /api/v1 mount). The outer mount may
                    # match, but endpoint params are defined in the nested app.
                    nested_scope = dict(scope)
                    nested_scope.update(child_scope)
                    nested_params = self._find_path_params(route_router.routes, nested_scope)
                    if nested_params is not None:
                        path_params.update(nested_params)
                        return path_params

            if match == Match.FULL:
                # FULL means this route resolves the current request path.
                # At this point path_params represent what endpoint dispatch
                # will use, so this is the correct branch to validate IDs.
                return path_params

        return None


class EventCounter:
    def __init__(self, window_seconds: int = 3600):
        self.window_seconds = window_seconds
        self.events: deque[float] = deque()

    def log_event(self):
        current_time = time.time()
        # Remove events older than the window
        while self.events and self.events[0] < current_time - self.window_seconds:
            self.events.popleft()
        # Add current event
        self.events.append(current_time)

    def get_count(self) -> int:
        current_time = time.time()
        # Remove old events and return count
        while self.events and self.events[0] < current_time - self.window_seconds:
            self.events.popleft()
        return len(self.events)


class HourlyLogCounter(EventCounter):
    def __init__(self):
        super().__init__(window_seconds=3600)
