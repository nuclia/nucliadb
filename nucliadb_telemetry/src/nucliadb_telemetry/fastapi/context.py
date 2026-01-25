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


def _get_header(scope, header_name: str) -> str | None:
    """Get a header value from ASGI scope"""
    headers = scope.get("headers")
    if not headers:
        return None

    # ASGI header keys are in lower case
    header_name = header_name.lower()
    for key, value in headers:
        if key.decode("utf8", errors="replace").lower() == header_name:
            return value.decode("utf8", errors="replace")
    return None


class ContextInjectorMiddleware:
    """
    Automatically inject context values for the current request's parameters, like path parameters or specific headers.

    For example:
        - `/api/v1/kb/{kbid}` would inject a context value for `kbid`
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            context_data = {}

            # Add path parameters
            found_path_template = get_path_template(scope)
            if found_path_template.match:
                context_data.update(found_path_template.scope.get("path_params", {}))  # type: ignore

            # Add user agent
            user_agent = _get_header(scope, "user-agent")
            if user_agent:
                context_data["user_agent"] = user_agent

            if context_data:
                context.add_context(context_data)

        return await self.app(scope, receive, send)
