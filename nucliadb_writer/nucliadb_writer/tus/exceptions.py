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
from typing import Optional

from starlette.exceptions import HTTPException as StarletteHTTPException


class InvalidTUSMetadata(Exception):
    pass


class HTTPException(StarletteHTTPException):
    _status_code: Optional[int] = None

    def __init__(self, detail: str = None):
        if self._status_code:
            super(HTTPException, self).__init__(
                status_code=self._status_code, detail=detail
            )
        else:
            raise AttributeError("Status code not defined")


class HTTPBadRequest(HTTPException):
    _status_code = 400


class HTTPForbidden(HTTPException):
    _status_code = 403


class HTTPNotFound(HTTPException):
    _status_code = 404


class HTTPConflict(HTTPException):
    _status_code = 409


class HTTPPreconditionFailed(HTTPException):
    _status_code = 412


class HTTPRangeNotSatisfiable(HTTPException):
    _status_code = 416


class HTTPClientClosedRequest(HTTPException):
    _status_code = 499


class HTTPServerError(HTTPException):
    _status_code = 500


class HTTPServiceUnavailable(HTTPException):
    _status_code = 503


class CloudFileNotFound(Exception):
    pass


class CloudFileAccessError(Exception):
    pass


class DMNotAvailable(Exception):
    pass


class ManagerNotAvailable(Exception):
    pass


class ResumableURINotAvailable(Exception):
    pass
