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
from starlette.requests import ClientDisconnect, Request
from starlette.responses import HTMLResponse

from nucliadb.train.api.v1.router import api
from nucliadb.train.lifecycle import lifespan
from nucliadb_telemetry import errors
from nucliadb_telemetry.fastapi.utils import (
    client_disconnect_handler,
    global_exception_handler,
)
from nucliadb_utils.audit.stream import AuditMiddleware
from nucliadb_utils.authentication import NucliaCloudAuthenticationBackend
from nucliadb_utils.fastapi.openapi import extend_openapi
from nucliadb_utils.settings import running_settings
from nucliadb_utils.utilities import get_audit

middleware = []
middleware.extend(
    [
        Middleware(AuthenticationMiddleware, backend=NucliaCloudAuthenticationBackend()),
        Middleware(AuditMiddleware, audit_utility_getter=get_audit),
    ]
)

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


application = FastAPI(title="NucliaDB Train API", **fastapi_settings)  # type: ignore

application.include_router(api)

extend_openapi(application)


async def homepage(request: Request) -> HTMLResponse:
    return HTMLResponse("NucliaDB Train Service")


# Use raw starlette routes to avoid unnecessary overhead
application.add_route("/", homepage)
