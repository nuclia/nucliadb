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

import functools
import inspect
import typing
from enum import Enum

from starlette.authentication import AuthCredentials, AuthenticationBackend, BaseUser
from starlette.exceptions import HTTPException
from starlette.requests import HTTPConnection, Request
from starlette.responses import RedirectResponse, Response
from starlette.websockets import WebSocket


class NucliaUser(BaseUser):
    def __init__(self, username: str, security_groups: list[str] | None = None) -> None:
        self.username = username
        self._security_groups = security_groups

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return self.username

    @property
    def security_groups(self) -> list[str] | None:
        return self._security_groups


STFUser = NucliaUser


class NucliaCloudAuthenticationBackend(AuthenticationBackend):
    def __init__(
        self,
        roles_header: str = "X-NUCLIADB-ROLES",
        user_header: str = "X-NUCLIADB-USER",
        security_groups_header: str = "X-NUCLIADB-SECURITY-GROUPS",
    ) -> None:
        self.roles_header = roles_header
        self.user_header = user_header
        self.security_groups_header = security_groups_header

    async def authenticate(self, conn: HTTPConnection) -> tuple[AuthCredentials, NucliaUser] | None:
        if self.roles_header not in conn.headers:
            return None
        else:
            header_roles = conn.headers[self.roles_header]
            roles = header_roles.split(";")
            auth_creds = AuthCredentials(roles)

        if self.user_header in conn.headers:
            user = conn.headers[self.user_header]

            raw_security_groups: str | None = conn.headers.get(self.security_groups_header)

            security_groups: list[str] | None = None
            if raw_security_groups is not None:
                security_groups = raw_security_groups.split(";")

            nuclia_user: NucliaUser = NucliaUser(username=user, security_groups=security_groups)

        else:
            nuclia_user = NucliaUser(username="anonymous")

        return auth_creds, nuclia_user


STFAuthenticationBackend = NucliaCloudAuthenticationBackend


def has_required_scope(conn: HTTPConnection, scopes: typing.Sequence[str]) -> bool:
    for scope in scopes:
        if scope in conn.auth.scopes:
            return True
    return False


def requires(
    scopes: str | typing.Sequence[str],
    status_code: int = 403,
    redirect: str | None = None,
) -> typing.Callable:
    # As a fastapi requirement, custom Enum classes have to inherit also from
    # string, so we MUST check for Enum before str
    if isinstance(scopes, Enum):
        scopes_list = [scopes.value]
    elif isinstance(scopes, str):
        scopes_list = [scopes]
    elif isinstance(scopes, list):
        scopes_list = [scope.value if isinstance(scope, Enum) else scope for scope in scopes]

    def decorator(func: typing.Callable) -> typing.Callable:
        func.__required_scopes__ = scopes_list  # type: ignore
        type = None
        sig = inspect.signature(func)
        for idx, parameter in enumerate(sig.parameters.values()):
            if parameter.name == "request" or parameter.name == "websocket":
                type = parameter.name
                break
        else:
            raise Exception(f'No "request" or "websocket" argument on function "{func}"')

        if type == "websocket":
            # Handle websocket functions. (Always async)
            @functools.wraps(func)
            async def websocket_wrapper(*args: typing.Any, **kwargs: typing.Any) -> None:
                websocket = kwargs.get("websocket", args[idx])
                assert isinstance(websocket, WebSocket)

                if not has_required_scope(websocket, scopes_list):
                    await websocket.close()
                else:
                    await func(*args, **kwargs)

            return websocket_wrapper

        elif inspect.iscoroutinefunction(func):
            # Handle async request/response functions.
            @functools.wraps(func)
            async def async_wrapper(*args: typing.Any, **kwargs: typing.Any) -> Response:
                request = kwargs.get("request", None)
                assert isinstance(request, Request)

                if not has_required_scope(request, scopes_list):
                    if redirect is not None:
                        return RedirectResponse(url=request.url_for(redirect), status_code=303)
                    raise HTTPException(status_code=status_code)
                return await func(*args, **kwargs)

            return async_wrapper

        else:
            # Handle sync request/response functions.
            @functools.wraps(func)
            def sync_wrapper(*args: typing.Any, **kwargs: typing.Any) -> Response:
                try:
                    request = kwargs["request"]
                except KeyError:
                    request = args[idx]
                assert isinstance(request, Request)

                if not has_required_scope(request, scopes_list):
                    if redirect is not None:
                        return RedirectResponse(url=request.url_for(redirect), status_code=303)
                    raise HTTPException(status_code=status_code)
                return func(*args, **kwargs)

            return sync_wrapper

    return decorator


# this code was an exact copy previously so get rid of it but keep b/w compat
requires_one = requires
