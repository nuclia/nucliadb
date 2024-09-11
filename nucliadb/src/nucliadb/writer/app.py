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

import importlib.metadata

from fastapi import FastAPI
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.requests import ClientDisconnect
from starlette.responses import HTMLResponse

from nucliadb.writer import API_PREFIX
from nucliadb.writer.api.v1.router import api as api_v1
from nucliadb.writer.lifecycle import lifespan
from nucliadb_telemetry import errors
from nucliadb_telemetry.fastapi.utils import (
    client_disconnect_handler,
    global_exception_handler,
)
from nucliadb_utils.authentication import NucliaCloudAuthenticationBackend
from nucliadb_utils.fastapi.openapi import extend_openapi
from nucliadb_utils.fastapi.versioning import VersionedFastAPI
from nucliadb_utils.settings import running_settings

middleware = []

middleware.extend([Middleware(AuthenticationMiddleware, backend=NucliaCloudAuthenticationBackend())])


errors.setup_error_handling(importlib.metadata.distribution("nucliadb").version)

fastapi_settings = dict(
    debug=running_settings.debug,
    middleware=middleware,
    lifespan=lifespan,
    exception_handlers={
        Exception: global_exception_handler,
        ClientDisconnect: client_disconnect_handler,
    },
)


def create_application() -> FastAPI:
    base_app = FastAPI(title="NucliaDB Writer API", **fastapi_settings)  # type: ignore

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

    async def homepage(request):
        return HTMLResponse("NucliaDB Writer Service")

    # Use raw starlette routes to avoid unnecessary overhead
    application.add_route("/", homepage)

    return application
