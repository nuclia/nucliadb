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

from fastapi.responses import JSONResponse
from sentry_sdk import capture_exception
from starlette.requests import ClientDisconnect, Request


async def global_exception_handler(request: Request, exc: Exception):
    capture_exception(exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Something went wrong, please contact your administrator"},
    )


async def client_disconnect_handler(request: Request, exc: ClientDisconnect):
    return JSONResponse(
        status_code=200,
        content={"detail": "Client disconnected while an operation was in course"},
    )


default_exception_handlers = {
    Exception: global_exception_handler,
    ClientDisconnect: client_disconnect_handler,
}
