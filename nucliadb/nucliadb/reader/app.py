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
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.aiohttp_client import (  # type: ignore
    AioHttpClientInstrumentor,
)
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat
from sentry_sdk import capture_exception
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.routing import Mount
from starlette_prometheus import PrometheusMiddleware

from nucliadb.reader import API_PREFIX, SERVICE_NAME
from nucliadb.reader.api.v1.router import api as api_v1
from nucliadb.reader.lifecycle import finalize, initialize
from nucliadb.sentry import SENTRY, set_sentry
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.authentication import STFAuthenticationBackend
from nucliadb_utils.fastapi.instrumentation import instrument_app
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
    Middleware(PrometheusMiddleware),
    Middleware(
        AuthenticationMiddleware,
        backend=STFAuthenticationBackend(),
    ),
]


if running_settings.sentry_url and SENTRY:
    set_sentry(
        running_settings.sentry_url,
        running_settings.running_environment,
        running_settings.logging_integration,
    )
    middleware.append(Middleware(SentryAsgiMiddleware))


on_startup = [initialize]
on_shutdown = [finalize]


async def global_exception_handler(request, exc):
    capture_exception(exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Something went wrong, please contact your administrator"},
    )


fastapi_settings = dict(
    debug=running_settings.debug,
    middleware=middleware,
    on_startup=on_startup,
    on_shutdown=on_shutdown,
    exception_handlers={Exception: global_exception_handler},
)


base_app = FastAPI(title="NucliaDB Reader API", **fastapi_settings)  # type: ignore

base_app.include_router(api_v1)

application = VersionedFastAPI(
    base_app,
    version_format="{major}",
    prefix_format=f"/{API_PREFIX}/v{{major}}",
    default_version=(1, 0),
    enable_latest=False,
    kwargs=fastapi_settings,
)

# Fastapi versioning does not propagate exception handlers to inner mounted apps
# We need to patch it manually for now. Also extend OpenAPI definitions
for route in application.routes:
    if isinstance(route, Mount):
        route.app.middleware_stack.handler = global_exception_handler  # type: ignore
        extend_openapi(route.app)  # type: ignore


async def homepage(request: Request) -> HTMLResponse:
    return HTMLResponse("NucliaDB Reader Service")


# Use raw starlette routes to avoid unnecessary overhead
application.add_route("/", homepage)

# Enable forwarding of B3 headers to responses and external requests
# to both inner applications
tracer_provider = get_telemetry(SERVICE_NAME)
set_global_textmap(B3MultiFormat())
instrument_app(application, tracer_provider=tracer_provider, excluded_urls=["/"])
AioHttpClientInstrumentor().instrument(tracer_provider=tracer_provider)
