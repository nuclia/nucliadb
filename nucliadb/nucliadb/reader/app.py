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
#
import pkg_resources
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import ClientDisconnect, Request
from starlette.responses import HTMLResponse

from nucliadb.reader import API_PREFIX
from nucliadb.reader.api.v1.router import api as api_v1
from nucliadb.reader.lifecycle import finalize, initialize
from nucliadb_telemetry import errors
from nucliadb_utils.authentication import NucliaCloudAuthenticationBackend
from nucliadb_utils.fastapi.openapi import extend_openapi
from nucliadb_utils.fastapi.versioning import VersionedFastAPI
from nucliadb_utils.settings import http_settings, running_settings

middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=http_settings.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    ),
    Middleware(
        AuthenticationMiddleware,
        backend=NucliaCloudAuthenticationBackend(),
    ),
]

errors.setup_error_handling(pkg_resources.get_distribution("nucliadb").version)

on_startup = [initialize]
on_shutdown = [finalize]


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


fastapi_settings = dict(
    debug=running_settings.debug,
    middleware=middleware,
    on_startup=on_startup,
    on_shutdown=on_shutdown,
    exception_handlers={
        Exception: global_exception_handler,
        ClientDisconnect: client_disconnect_handler,
    },
)


base_app = FastAPI(title="NucliaDB Reader API", **fastapi_settings)  # type: ignore

base_app.include_router(api_v1)

extend_openapi(base_app)

application = VersionedFastAPI(
    base_app,
    version_format="{major}",
    prefix_format=f"/{API_PREFIX}/v{{major}}",
    default_version=(1, 0),
    enable_latest=False,
    kwargs=fastapi_settings,
)


async def homepage(request: Request) -> HTMLResponse:
    return HTMLResponse("NucliaDB Reader Service")


# Use raw starlette routes to avoid unnecessary overhead
application.add_route("/", homepage)
