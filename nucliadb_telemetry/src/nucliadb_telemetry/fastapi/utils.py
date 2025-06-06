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
from typing import List, NamedTuple, Optional, Tuple

from fastapi.responses import JSONResponse
from starlette.applications import Starlette
from starlette.datastructures import URL
from starlette.requests import ClientDisconnect, Request
from starlette.routing import Match, Mount, Route
from starlette.types import Scope

from nucliadb_telemetry import errors


class FoundPathTemplate(NamedTuple):
    path: str
    scope: Optional[Scope]
    match: bool


def get_path_template(scope: Scope) -> FoundPathTemplate:
    if "found_path_template" in scope:
        return scope["found_path_template"]
    app: Starlette = scope["app"]
    path, sub_scope = find_route(scope, app.routes)  # type:ignore
    if path is None:
        path = URL(scope=scope).path
    path_template = FoundPathTemplate(path, sub_scope, sub_scope is not None)
    scope["found_path_template"] = path_template
    return path_template


def find_route(scope: Scope, routes: List[Route]) -> Tuple[Optional[str], Optional[Scope]]:
    # we mutate scope, so we need a copy
    scope = scope.copy()  # type:ignore
    for route in routes:
        if isinstance(route, Mount):
            mount_match, child_scope = route.matches(scope)
            if mount_match == Match.FULL:
                scope.update(child_scope)
                sub_path, sub_match = find_route(scope, route.routes)
                if sub_match and sub_path is not None:
                    return route.path + sub_path, sub_match
        elif isinstance(route, Route):
            match, child_scope = route.matches(scope)
            if match == Match.FULL:
                return route.path, child_scope
    return None, None


async def global_exception_handler(request: Request, exc: Exception):
    errors.capture_exception(exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Something went wrong, please contact your administrator"},
    )


async def client_disconnect_handler(request: Request, exc: ClientDisconnect):
    return JSONResponse(
        status_code=200,
        content={"detail": "Client disconnected while an operation was in course"},
    )
