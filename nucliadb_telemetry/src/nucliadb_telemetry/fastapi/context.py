# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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
