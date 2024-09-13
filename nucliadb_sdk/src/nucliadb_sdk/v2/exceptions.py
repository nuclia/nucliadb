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
from typing import Optional


class ClientError(Exception):
    pass


class NotFoundError(ClientError):
    pass


class AuthError(ClientError):
    pass


class AccountLimitError(ClientError):
    pass


class RateLimitError(ClientError):
    def __init__(self, message, try_after: Optional[float] = None):
        super().__init__(message)
        self.try_after = try_after


class ConflictError(ClientError):
    pass


class UnknownError(ClientError):
    pass


class AskResponseError(ClientError):
    pass
