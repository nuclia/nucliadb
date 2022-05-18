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
import prometheus_client  # type: ignore
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi_versioning import VersionedFastAPI
from opentelemetry.instrumentation.aiohttp_client import (  # type: ignore
    AioHttpClientInstrumentor,
)
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse, PlainTextResponse
from starlette.routing import Mount
from starlette_prometheus import PrometheusMiddleware

from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.authentication import STFAuthenticationBackend
from nucliadb_utils.settings import http_settings, running_settings
from nucliadb_writer import API_PREFIX, SERVICE_NAME
from nucliadb_writer.api.v1.router import api as api_v1
from nucliadb_writer.lifecycle import finalize, initialize
from nucliadb_writer.sentry import set_sentry

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


if running_settings.sentry_url:
    set_sentry(
        running_settings.sentry_url,
        running_settings.running_environment,
        running_settings.logging_integration,
    )
    middleware.append(Middleware(SentryAsgiMiddleware))

on_startup = [initialize]
on_shutdown = [finalize]


async def global_exception_handler(request, exc):
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

base_app = FastAPI(title="NucliaDB Writer API", **fastapi_settings)  # type: ignore

base_app.include_router(api_v1)

application = VersionedFastAPI(
    base_app,
    version_format="{major}",
    prefix_format=f"/{API_PREFIX}/v{{major}}",
    **fastapi_settings,
)

# Fastapi versioning does not propagate exception handlers to inner mounted apps
# We need to patch it manually for now
for route in application.routes:
    if isinstance(route, Mount):
        route.app.middleware_stack.handler = global_exception_handler  # type: ignore


async def homepage(request):
    return HTMLResponse("NucliaDB Writer Service")


async def metrics(request):
    output = prometheus_client.exposition.generate_latest()
    return PlainTextResponse(output.decode("utf8"))


# Use raw starlette routes to avoid unnecessary overhead
application.add_route("/", homepage)
application.add_route("/metrics", metrics)
# Enable forwarding of B3 headers to responses and external requests
# to both inner applications

tracer_provider = None
if telemetry_settings.jeager_enabled:
    tracer_provider = get_telemetry(SERVICE_NAME)

set_global_textmap(B3MultiFormat())
FastAPIInstrumentor.instrument_app(application, tracer_provider=tracer_provider)
AioHttpClientInstrumentor().instrument(tracer_provider=tracer_provider)
