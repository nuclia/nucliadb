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
from fastapi.responses import StreamingResponse
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.common.context.fastapi import get_app_context
from nucliadb.export_import import exporter
from nucliadb.reader.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires_one


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/export",
    status_code=200,
    name="Export a Knowledge Box",
    tags=["Knowledge Boxes"],
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.READER])
@version(1)
async def export_kb_endpoint(request: Request, kbid: str) -> StreamingResponse:
    context = get_app_context(request.app)
    return StreamingResponse(
        exporter.export_kb(context, kbid),
        status_code=200,
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename={kbid}.export"},
    )
