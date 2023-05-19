from starlette.routing import Match, Mount, Route
from starlette.types import Scope
from typing import Optional, NamedTuple
from starlette.applications import Starlette
from starlette.datastructures import URL


class FoundPathTemplate(NamedTuple):
    path: str
    scope: Optional[Scope]
    match: bool


def get_path_template(scope: Scope) -> FoundPathTemplate:
    if "found_path_template" in scope:
        return scope["found_path_template"]
    app: Starlette = scope["app"]
    path, sub_scope = find_route(scope, app.routes)
    if path is None:
        path = URL(scope=scope).path
    path_template = FoundPathTemplate(path, sub_scope, sub_scope is not None)
    scope["found_path_template"] = path_template
    return path_template


def find_route(
    scope: Scope, routes: list[Route]
) -> tuple[Optional[str], Optional[Scope]]:
    # we mutate scope, so we need a copy
    scope = scope.copy()  # type:ignore
    for route in routes:
        if isinstance(route, Mount):
            mount_match, child_scope = route.matches(scope)
            if mount_match == Match.FULL:
                scope.update(child_scope)
                sub_path, sub_match = find_route(scope, route.routes)
                if sub_match:
                    return route.path + sub_path, sub_match
        elif isinstance(route, Route):
            match, child_scope = route.matches(scope)
            if match == Match.FULL:
                return route.path, child_scope
    return None, None
