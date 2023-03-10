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

from typing import Union

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


def extend_openapi(app: Union[FastAPI, Mount]):  # pragma: no cover
    for route in app.routes:
        # mypy complains about BaseRoute not having endpoint and
        # description attributes, but routes passed here always have
        if hasattr(route.endpoint, "__required_scopes__"):  # type: ignore
            scopes = route.endpoint.__required_scopes__  # type: ignore
            route.description += ROLE_METADATA_TEMPLATE.format(  # type: ignore
                scopes=format_scopes(scopes)
            )
