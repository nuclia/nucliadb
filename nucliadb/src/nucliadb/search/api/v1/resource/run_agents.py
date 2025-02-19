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
from fastapi import Request, Response
from fastapi_versioning import version

from nucliadb.search.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCE_SLUG_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    ResourceSearchResults,
)
from nucliadb_utils.authentication import requires_one


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/run-agents",
    status_code=200,
    summary="Search on Resource",
    description="Search on a single resource",
    tags=["Search"],
    response_model_exclude_unset=True,
    response_model=ResourceSearchResults,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def run_agents_by_uuid(
    request: Request,
    response: Response,
    kbid: str,
    rid: str,
):
    pass


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_SLUG_PREFIX}/{{slug}}/run-agents",
    status_code=200,
    summary="Search on Resource",
    description="Search on a single resource",
    tags=["Search"],
    response_model_exclude_unset=True,
    response_model=ResourceSearchResults,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def run_agents_by_slug(
    request: Request,
    response: Response,
    kbid: str,
    slug: str,
):
    pass
