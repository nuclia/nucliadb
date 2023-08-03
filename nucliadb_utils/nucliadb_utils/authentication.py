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

import asyncio
import functools
import inspect
import typing
from enum import Enum
from typing import Optional, Tuple

from starlette.authentication import AuthCredentials, AuthenticationBackend, BaseUser
from starlette.exceptions import HTTPException
from starlette.requests import HTTPConnection, Request
from starlette.responses import RedirectResponse, Response
from starlette.websockets import WebSocket


class NucliaUser(BaseUser):
    def __init__(self, username: str) -> None:
        self.username = username

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return self.username


STFUser = NucliaUser


class NucliaCloudAuthenticationBackend(AuthenticationBackend):
    def __init__(
        self,
        roles_header: str = "X-NUCLIADB-ROLES",
        user_header: str = "X-NUCLIADB-USER",
    ) -> None:
        self.roles_header = roles_header
        self.user_header = user_header

    async def authenticate(
        self, request: HTTPConnection
    ) -> Optional[Tuple[AuthCredentials, BaseUser]]:
        if self.roles_header not in request.headers:
            return None
        else:
            header_roles = request.headers[self.roles_header]
            roles = header_roles.split(";")
            auth_creds = AuthCredentials(roles)

        if self.user_header in request.headers:
            user = request.headers[self.user_header]
            nuclia_user: BaseUser = NucliaUser(username=user)
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
    scopes: typing.Union[str, typing.Sequence[str]],
    status_code: int = 403,
    redirect: Optional[str] = None,
) -> typing.Callable:
    # As a fastapi requirement, custom Enum classes have to inherit also from
    # string, so we MUST check for Enum before str
    if isinstance(scopes, Enum):
        scopes_list = [scopes.value]
    elif isinstance(scopes, str):
        scopes_list = [scopes]
    elif isinstance(scopes, list):
        scopes_list = [
            scope.value if isinstance(scope, Enum) else scope for scope in scopes
        ]

    def decorator(func: typing.Callable) -> typing.Callable:
        func.__required_scopes__ = scopes_list  # type: ignore
        type = None
        sig = inspect.signature(func)
        for idx, parameter in enumerate(sig.parameters.values()):
            if parameter.name == "request" or parameter.name == "websocket":
                type = parameter.name
                break
        else:
            raise Exception(
                f'No "request" or "websocket" argument on function "{func}"'
            )

        if type == "websocket":
            # Handle websocket functions. (Always async)
            @functools.wraps(func)
            async def websocket_wrapper(
                *args: typing.Any, **kwargs: typing.Any
            ) -> None:
                websocket = kwargs.get("websocket", args[idx])
                assert isinstance(websocket, WebSocket)

                if not has_required_scope(websocket, scopes_list):
                    await websocket.close()
                else:
                    await func(*args, **kwargs)

            return websocket_wrapper

        elif asyncio.iscoroutinefunction(func):
            # Handle async request/response functions.
            @functools.wraps(func)
            async def async_wrapper(
                *args: typing.Any, **kwargs: typing.Any
            ) -> Response:
                request = kwargs.get("request", None)
                assert isinstance(request, Request)

                if not has_required_scope(request, scopes_list):
                    if redirect is not None:
                        return RedirectResponse(
                            url=request.url_for(redirect), status_code=303
                        )
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
                        return RedirectResponse(
                            url=request.url_for(redirect), status_code=303
                        )
                    raise HTTPException(status_code=status_code)
                return func(*args, **kwargs)

            return sync_wrapper

    return decorator


# this code was an exact copy previously so get rid of it but keep b/w compat
requires_one = requires
