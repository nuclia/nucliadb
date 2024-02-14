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
from fastapi import Request
from fastapi_versioning import version

from nucliadb import learning_config
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires


@api.post(
    path=f"/{KB_PREFIX}/{{kbid}}/configuration",
    status_code=204,
    name="Create Knowledge Box models configuration",
    description="Create configuration of models assigned to a Knowledge Box",
    response_model=None,
    tags=["Knowledge Boxes"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def set_configuration(
    request: Request,
    kbid: str,
):
    return await learning_config.proxy(request, "POST", f"/config/{kbid}")


@api.patch(
    path=f"/{KB_PREFIX}/{{kbid}}/configuration",
    status_code=204,
    name="Update Knowledge Box models configuration",
    description="Update current configuration of models assigned to a Knowledge Box",
    response_model=None,
    tags=["Knowledge Boxes"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def patch_configuration(
    request: Request,
    kbid: str,
):
    return await learning_config.proxy(request, "PATCH", f"/config/{kbid}")
