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


class Response(JSONResponse):
    pass


class HTTPClientError(Response):
    def __init__(self, status_code: int, detail: str):
        super().__init__(status_code=status_code, content={"detail": detail})


class HTTPNotFound(HTTPClientError):
    status_code = 404

    def __init__(self, detail: str):
        super().__init__(status_code=self.status_code, detail=detail)


class HTTPConflict(HTTPClientError):
    status_code = 409

    def __init__(self, detail: str):
        super().__init__(status_code=self.status_code, detail=detail)


class HTTPInternalServerError(Response):
    status_code = 500

    def __init__(self, detail: str):
        super().__init__(content=detail, status_code=self.status_code)
