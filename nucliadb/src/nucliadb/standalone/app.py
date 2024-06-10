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
import logging
import os

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import ClientDisconnect
from starlette.responses import HTMLResponse
from starlette.routing import Mount

import nucliadb_admin_assets  # type: ignore
from nucliadb.common.context.fastapi import set_app_context
from nucliadb.middleware import ProcessTimeHeaderMiddleware
from nucliadb.middleware.transaction import ReadOnlyTransactionMiddleware
from nucliadb.reader import API_PREFIX
from nucliadb.reader.api.v1.router import api as api_reader_v1
from nucliadb.search.api.v1.router import api as api_search_v1
from nucliadb.standalone.lifecycle import finalize, initialize
from nucliadb.train.api.v1.router import api as api_train_v1
from nucliadb.writer.api.v1.router import api as api_writer_v1
from nucliadb_telemetry.fastapi import metrics_endpoint
from nucliadb_telemetry.fastapi.utils import (
    client_disconnect_handler,
    global_exception_handler,
)
from nucliadb_utils.fastapi.openapi import extend_openapi
from nucliadb_utils.fastapi.versioning import VersionedFastAPI
from nucliadb_utils.settings import http_settings, running_settings

from .api_router import standalone_api_router
from .auth import get_auth_backend
from .settings import Settings

logger = logging.getLogger(__name__)


HOMEPAGE_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NucliaDB Standalone Server</title>
</head>
<body>
    <h1>Welcome to NucliaDB Standalone Server</h1>
    <p> The NucliaDB API is exposed at /api/v1. </p>
    <br>
    <h2>Quick Links</h2>
    <ul>
        <li><a href="/admin">Admin UI</a></li>
        <li><a href="https://docs.nuclia.dev/docs/guides/nucliadb/deploy/basics">NucliaDB Deployment Documentation</a></li>
        <li><a href="https://docs.nuclia.dev/docs/api">API Reference</a></li>
        <li><a href="/api/v1/docs">API Explorer</a></li>
        <li><a href="/metrics">Metrics</a></li>
        <li><a href="https://docs.nuclia.dev/docs/">Nuclia Documentation</a></li>
    </ul>
</body>
</html>
"""  # noqa: E501


def application_factory(settings: Settings) -> FastAPI:
    middleware = [
        Middleware(
            CORSMiddleware,
            allow_origins=http_settings.cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        ),
        Middleware(
            AuthenticationMiddleware,
            backend=get_auth_backend(settings),
        ),
        Middleware(ReadOnlyTransactionMiddleware),
    ]
    if running_settings.debug:
        middleware.append(Middleware(ProcessTimeHeaderMiddleware))

    fastapi_settings = dict(
        debug=running_settings.debug,
        middleware=middleware,
        on_startup=[initialize],
        on_shutdown=[finalize],
        exception_handlers={
            Exception: global_exception_handler,
            ClientDisconnect: client_disconnect_handler,
        },
    )

    base_app = FastAPI(title="NucliaDB API", **fastapi_settings)  # type: ignore
    base_app.include_router(api_writer_v1)
    base_app.include_router(api_reader_v1)
    base_app.include_router(api_search_v1)
    base_app.include_router(api_train_v1)
    base_app.include_router(standalone_api_router)

    application = VersionedFastAPI(
        base_app,
        version_format="{major}",
        prefix_format=f"/{API_PREFIX}/v{{major}}",
        default_version=(1, 0),
        enable_latest=False,
        kwargs=fastapi_settings,
    )

    for route in application.routes:
        if isinstance(route, Mount):
            extend_openapi(route)

    async def homepage(request):
        return HTMLResponse(HOMEPAGE_HTML)

    # Use raw starlette routes to avoid unnecessary overhead
    application.add_route("/", homepage)
    application.add_route("/metrics", metrics_endpoint)

    # mount admin app assets
    application.mount(
        "/admin",
        StaticFiles(directory=os.path.dirname(nucliadb_admin_assets.__file__), html=True),
        name="static",
    )
    # redirect /contributor -> /admin
    application.add_route("/contributor", lambda request: RedirectResponse("/admin"))
    application.mount(
        "/widget",
        StaticFiles(directory=os.path.dirname(__file__) + "/static", html=True),
        name="widget",
    )

    application.settings = settings  # type: ignore
    for route in application.router.routes:
        if isinstance(route, Mount):
            route.app.settings = settings  # type: ignore

    # Inject application context into the fastapi app's state
    set_app_context(application)

    return application
