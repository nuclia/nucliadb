from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from nucliadb_telemetry import context

from .utils import get_path_template


class ContextInjectorMiddleware(BaseHTTPMiddleware):
    """
    Automatically inject context values for the current request's path parameters

    For example:
        - `/api/v1/kb/{kbid}` would inject a context value for `kbid`
    """

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        found_path_template = get_path_template(request.scope)
        if found_path_template.match:
            context.add_context(found_path_template.scope.get("path_params", {}))

        return await call_next(request)
