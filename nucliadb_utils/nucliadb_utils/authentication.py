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


class STFUser(BaseUser):
    def __init__(self, username: str) -> None:
        self.username = username

    @property
    def is_authenticated(self) -> bool:
        return True

    @property
    def display_name(self) -> str:
        return self.username


class STFAuthenticationBackend(AuthenticationBackend):
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
            auth_creds = None
            return None
        else:
            header_roles = request.headers[self.roles_header]
            roles = header_roles.split(";")
            auth_creds = AuthCredentials(roles)

        if self.user_header in request.headers:
            user = request.headers[self.user_header]
            stf_user: BaseUser = STFUser(username=user)
        else:
            stf_user = STFUser(username="anonymous")

        return auth_creds, stf_user


def has_required_scope(conn: HTTPConnection, scopes: typing.Sequence[str]) -> bool:
    for scope in scopes:
        if scope in conn.auth.scopes:
            return True
    return False


def requires(
    scopes: typing.Union[str, typing.Sequence[str]],
    status_code: int = 403,
    redirect: str = None,
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
                request = kwargs.get("request", args[idx])
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


def requires_one(
    scopes: typing.Union[str, typing.Sequence[str]],
    status_code: int = 403,
    redirect: str = None,
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
                request = kwargs.get("request", args[idx])
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
