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

from nucliadb_telemetry import context

from .utils import get_path_template


class ContextInjectorMiddleware:
    """
    Automatically inject context values for the current request's path parameters

    For example:
        - `/api/v1/kb/{kbid}` would inject a context value for `kbid`
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            found_path_template = get_path_template(scope)
            if found_path_template.match:
                context.add_context(found_path_template.scope.get("path_params", {}))  # type: ignore

        return await self.app(scope, receive, send)
