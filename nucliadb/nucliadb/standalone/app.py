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

import nucliadb_contributor_assets  # type: ignore
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse
from starlette.routing import Mount

from nucliadb.reader import API_PREFIX
from nucliadb.reader.api.v1.router import api as api_reader_v1
from nucliadb.search.api.v1.router import api as api_search_v1
from nucliadb.standalone.lifecycle import finalize, initialize
from nucliadb.train.api.v1.router import api as api_train_v1
from nucliadb.writer.api.v1.router import api as api_writer_v1
from nucliadb_telemetry.fastapi import metrics_endpoint
from nucliadb_utils.fastapi.openapi import extend_openapi
from nucliadb_utils.fastapi.versioning import VersionedFastAPI
from nucliadb_utils.settings import http_settings, running_settings

from .api_router import standalone_api_router
from .auth import get_auth_backend
from .settings import Settings

logger = logging.getLogger(__name__)


def application_factory(settings: Settings) -> FastAPI:
    fastapi_settings = dict(
        debug=running_settings.debug,
        middleware=[
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
        ],
        on_startup=[initialize],
        on_shutdown=[finalize],
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
        return HTMLResponse("NucliaDB Standalone Server")

    # Use raw starlette routes to avoid unnecessary overhead
    application.add_route("/", homepage)
    application.add_route("/metrics", metrics_endpoint)

    # mount contributor app assets
    application.mount(
        "/contributor",
        StaticFiles(
            directory=os.path.dirname(nucliadb_contributor_assets.__file__), html=True
        ),
        name="static",
    )
    application.mount(
        "/widget",
        StaticFiles(directory=os.path.dirname(__file__) + "/static", html=True),
        name="widget",
    )

    return application
