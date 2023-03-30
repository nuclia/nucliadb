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
from fastapi.responses import JSONResponse


class HTTPResponse(JSONResponse):
    def __init__(self, detail: str):
        super().__init__(content={"detail": detail}, status_code=self.status_code)


class HTTPNotFound(HTTPResponse):
    status_code = 404


class HTTPConflict(HTTPResponse):
    status_code = 409


class HTTPUnprocessableEntity(HTTPResponse):
    status_code = 422


class HTTPInternalServerError(HTTPResponse):
    status_code = 500


class HTTPServiceUnavailableError(HTTPResponse):
    status_code = 500
