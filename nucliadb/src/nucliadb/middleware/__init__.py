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

import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

PROCESS_TIME_HEADER = "X-PROCESS-TIME"
ACCESS_CONTROL_EXPOSE_HEADER = "Access-Control-Expose-Headers"


class ProcessTimeHeaderMiddleware(BaseHTTPMiddleware):
    def capture_process_time(self, response, duration: float):
        response.headers[PROCESS_TIME_HEADER] = str(duration)

    def expose_process_time_header(self, response):
        exposed_headers = []
        if ACCESS_CONTROL_EXPOSE_HEADER in response.headers:
            exposed_headers = response.headers[ACCESS_CONTROL_EXPOSE_HEADER].split(",")
        if PROCESS_TIME_HEADER not in exposed_headers:
            exposed_headers.append(PROCESS_TIME_HEADER)
            response.headers[ACCESS_CONTROL_EXPOSE_HEADER] = ",".join(exposed_headers)

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        response = None
        start = time.perf_counter()
        try:
            response = await call_next(request)
        finally:
            if response is not None:
                duration = time.perf_counter() - start
                self.capture_process_time(response, duration)
                self.expose_process_time_header(response)
                return response
