# Copyright 2021 Bosutech XXI S.L.
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

# This code is inspired by fastapi_versioning 1/3/2022 with MIT licence

from collections import defaultdict
from collections.abc import Callable
from typing import Any, Sequence, TypeVar, cast

from fastapi import FastAPI
from fastapi.middleware import Middleware
from fastapi.routing import APIRoute
from starlette.routing import BaseRoute

CallableT = TypeVar("CallableT", bound=Callable[..., Any])


def version(major: int, minor: int = 0) -> Callable[[CallableT], CallableT]:  # pragma: no cover
    def decorator(func: CallableT) -> CallableT:
        func._api_version = (major, minor)  # type: ignore[attr-defined,ty:unresolved-attribute]
        return func

    return decorator


def version_to_route(
    route: BaseRoute,
    default_version: tuple[int, int],
) -> tuple[tuple[int, int], APIRoute]:  # pragma: no cover
    api_route = cast(APIRoute, route)
    version = getattr(api_route.endpoint, "_api_version", default_version)
    return version, api_route


def VersionedFastAPI(
    app: FastAPI,
    version_format: str = "{major}.{minor}",
    prefix_format: str = "/v{major}_{minor}",
    default_version: tuple[int, int] = (1, 0),
    enable_latest: bool = False,
    middleware: Sequence[Middleware] | None = None,
    kwargs: dict[str, object] | None = None,
) -> FastAPI:  # pragma: no cover
    kwargs = kwargs or {}

    parent_app = FastAPI(
        title=app.title,
        middleware=middleware,
        **kwargs,  # type: ignore
    )
    version_route_mapping: dict[tuple[int, int], list[APIRoute]] = defaultdict(list)
    version_routes = [version_to_route(route, default_version) for route in app.routes]

    for version, route in version_routes:
        version_route_mapping[version].append(route)

    unique_routes = {}
    versions = sorted(version_route_mapping.keys())
    for version in versions:
        major, minor = version
        prefix = prefix_format.format(major=major, minor=minor)
        semver = version_format.format(major=major, minor=minor)
        versioned_app = FastAPI(
            title=app.title,
            description=app.description,
            version=semver,
            **kwargs,  # type: ignore
        )
        for route in version_route_mapping[version]:
            for method in route.methods:
                unique_routes[route.path + "|" + method] = route
        for route in unique_routes.values():
            versioned_app.router.routes.append(route)
        parent_app.mount(prefix, versioned_app)

        @parent_app.get(f"{prefix}/openapi.json", name=semver, tags=["Versions"])
        @parent_app.get(f"{prefix}/docs", name=semver, tags=["Documentations"])
        def noop() -> None: ...

    if enable_latest:
        prefix = "/latest"
        major, minor = version
        semver = version_format.format(major=major, minor=minor)
        versioned_app = FastAPI(
            title=app.title,
            description=app.description,
            version=semver,
        )
        for route in unique_routes.values():
            versioned_app.router.routes.append(route)
        parent_app.mount(prefix, versioned_app)

    return parent_app
