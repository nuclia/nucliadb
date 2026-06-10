# Copyright 2021 Bosutech XXI S.L.
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


from fastapi.applications import FastAPI
from starlette.routing import Mount

ROLE_METADATA_TEMPLATE = """
---
## Authorization roles
Authenticated user needs to fulfill one of this roles, otherwise the request will be rejected with a `403` response.
{scopes}
"""


def format_scopes(scope_list):
    return "\n".join(f"- `{scope}`" for scope in scope_list)


def extend_openapi(app: FastAPI | Mount):  # pragma: no cover
    for route in app.routes:
        # mypy complains about BaseRoute not having endpoint and
        # description attributes, but routes passed here always have
        if hasattr(route.endpoint, "__required_scopes__"):  # type: ignore
            scopes = route.endpoint.__required_scopes__  # type: ignore
            route.description += ROLE_METADATA_TEMPLATE.format(  # type: ignore
                scopes=format_scopes(scopes)
            )
