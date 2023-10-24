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
from typing import List, NamedTuple, Optional, Tuple

from starlette.applications import Starlette
from starlette.datastructures import URL
from starlette.routing import Match, Mount, Route
from starlette.types import Scope


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


def find_route(
    scope: Scope, routes: List[Route]
) -> Tuple[Optional[str], Optional[Scope]]:
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
