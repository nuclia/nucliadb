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
import httpx


class MarkLogicError(Exception):
    pass


class MarkLogicResponseError(MarkLogicError):
    def __init__(self, message: str, *, response: httpx.Response | None = None, code: str | None = None):
        super().__init__(message)
        self.response = response
        self.status_code = response.status_code if response is not None else None
        self.code = code


class MarkLogicProtocolError(MarkLogicError):
    """The server response does not match the expected REST protocol."""


class NoHealthyUpstreamError(MarkLogicError):
    """Raised when MarkLogic returns 503 'no healthy upstream'.

    This is a transient load-balancer error that can be safely retried.
    """

    pass


class DatabaseDoesNotExist(MarkLogicResponseError):
    """Raised when MarkLogic reports that the target database no longer exists."""

    pass


class RoleDoesNotExist(MarkLogicError):
    """Raised when MarkLogic reports SEC-ROLEDNE: a referenced role no longer exists.

    This commonly happens when a document write races with project deletion: project
    roles (e.g. ``project-{id}-reader``/``-writer``) are deleted as the final step of
    project cleanup, so a write that started earlier (e.g. a long-running background
    pipeline) can still be in flight and reference those roles by the time it finishes.

    Can also happen when a role is created via one host and immediately referenced
    in a subsequent call to another host, who may not yet be on the same server timestamp
    as the first host.
    """

    pass
