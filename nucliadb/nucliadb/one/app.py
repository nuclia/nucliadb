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
import os

import aiohttp
import nucliadb_contributor_assets  # type: ignore
import pkg_resources
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse
from starlette.routing import Mount

from nucliadb.ingest.utils import get_processing_api_url
from nucliadb.one.lifecycle import finalize, initialize
from nucliadb.reader import API_PREFIX
from nucliadb.reader.api.v1.router import api as api_reader_v1
from nucliadb.search.api.v1.router import api as api_search_v1
from nucliadb.train.api.v1.router import api as api_train_v1
from nucliadb.writer.api.v1.router import api as api_writer_v1
from nucliadb_telemetry import errors
from nucliadb_telemetry.fastapi import instrument_app, metrics_endpoint
from nucliadb_utils.authentication import STFAuthenticationBackend
from nucliadb_utils.fastapi.openapi import extend_openapi
from nucliadb_utils.fastapi.versioning import VersionedFastAPI
from nucliadb_utils.settings import http_settings, nuclia_settings, running_settings

middleware = [
    Middleware(
        CORSMiddleware,
        allow_origins=http_settings.cors_origins,
        allow_methods=["*"],
        allow_headers=["*"],
    ),
    Middleware(
        AuthenticationMiddleware,
        backend=STFAuthenticationBackend(),
    ),
]


errors.setup_error_handling(pkg_resources.get_distribution("nucliadb").version)


on_startup = [initialize]
on_shutdown = [finalize]


fastapi_settings = dict(
    debug=running_settings.debug,
    middleware=middleware,
    on_startup=on_startup,
    on_shutdown=on_shutdown,
)


base_app = FastAPI(title="NucliaDB API", **fastapi_settings)  # type: ignore

base_app.include_router(api_writer_v1)
base_app.include_router(api_reader_v1)
base_app.include_router(api_search_v1)
base_app.include_router(api_train_v1)

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


async def api_config_check(request):
    valid_nua_key = False
    nua_key_check_error = None
    if nuclia_settings.nuclia_service_account is not None:
        processing_url = get_processing_api_url()
        url = processing_url + "/status"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={
                    "X-STF-NUAKEY": f"Bearer {nuclia_settings.nuclia_service_account}"
                },
            ) as resp:
                if resp.status == 200:
                    valid_nua_key = True
                else:
                    resp_text = await resp.text()
                    nua_key_check_error = (
                        f"Error checking NUA key: {resp.status} - {resp_text}"
                    )
    return JSONResponse(
        {
            "nua_api_key": {
                "has_key": nuclia_settings.nuclia_service_account is not None,
                "valid": valid_nua_key,
                "error": nua_key_check_error,
            }
        }
    )


# Use raw starlette routes to avoid unnecessary overhead
application.add_route("/", homepage)
application.add_route("/config-check", api_config_check)
application.add_route("/metrics", metrics_endpoint)

# mount contributor app assets
application.mount(
    "/contributor",
    StaticFiles(
        directory=os.path.dirname(nucliadb_contributor_assets.__file__), html=True
    ),
    name="static",
)


instrument_app(application, excluded_urls=["/"], metrics=True)
