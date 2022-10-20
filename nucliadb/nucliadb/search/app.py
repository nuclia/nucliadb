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

import prometheus_client  # type: ignore
from fastapi import FastAPI
from opentelemetry.instrumentation.aiohttp_client import (  # type: ignore
    AioHttpClientInstrumentor,
)
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware
from starlette.middleware import Middleware
from starlette.middleware.authentication import AuthenticationMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse
from starlette.routing import Mount
from starlette_prometheus import PrometheusMiddleware

from nucliadb.ingest.orm import NODES
from nucliadb.search import API_PREFIX, SERVICE_NAME
from nucliadb.search.api.v1.router import api as api_v1
from nucliadb.search.lifecycle import finalize, initialize
from nucliadb.search.utilities import get_counter
from nucliadb.sentry import SENTRY, set_sentry
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.authentication import STFAuthenticationBackend
from nucliadb_utils.fastapi.instrumentation import instrument_app
from nucliadb_utils.fastapi.openapi import extend_openapi
from nucliadb_utils.fastapi.versioning import VersionedFastAPI
from nucliadb_utils.settings import http_settings, running_settings

logging.getLogger("nucliadb_chitchat").setLevel(
    logging.getLevelName(running_settings.chitchat_level.upper())
)

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
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


fastapi_settings = dict(
    debug=running_settings.debug,
    middleware=middleware,
    on_startup=on_startup,
    on_shutdown=on_shutdown,
    exception_handlers={Exception: global_exception_handler},
)


base_app = FastAPI(title="NucliaDB Search API", **fastapi_settings)  # type: ignore

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
    return HTMLResponse("NucliaDB Search Service")


async def metrics(request: Request) -> PlainTextResponse:
    output = prometheus_client.exposition.generate_latest()
    return PlainTextResponse(output.decode("utf8"))


async def chitchat_members(request: Request) -> JSONResponse:
    return JSONResponse(
        [
            {
                "id": node_id,
                "listen_address": node.address,
                "type": node.label,
                "dummy": node.dummy,
            }
            for node_id, node in NODES.items()
        ]
    )


async def accounting(request: Request) -> JSONResponse:
    search_counter = get_counter()
    response = dict(search_counter)
    search_counter.clear()
    return JSONResponse(response)


# Use raw starlette routes to avoid unnecessary overhead
application.add_route("/", homepage)
application.add_route("/metrics", metrics)
application.add_route("/chitchat/members", chitchat_members)
application.add_route("/accounting", accounting)

# Enable forwarding of B3 headers to responses and external requests
# to both inner applications
tracer_provider = get_telemetry(SERVICE_NAME)
set_global_textmap(B3MultiFormat())
instrument_app(application, tracer_provider=tracer_provider, excluded_urls=["/"])
AioHttpClientInstrumentor().instrument(tracer_provider=tracer_provider)
