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

from fastapi import Path
from fastapi.requests import Request
from fastapi.responses import Response
from fastapi_versioning import version

from nucliadb.common import file_md5
from nucliadb.reader.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires_one


@api.head(
    f"/{KB_PREFIX}/{{kbid}}/file-exists/{{md5}}",
    status_code=200,
    summary="Check if a file with the given MD5 hash already exists in the knowledge box",
    tags=["Knowledge Box Services"],
    responses={404: {"description": "File with the given MD5 hash not found"}},
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def file_exists(
    request: Request,
    kbid: str,
    md5: str = Path(..., pattern=r"^[a-fA-F0-9]{32}$", description="MD5 hash of the file to check"),
):
    exists = await file_md5.exists(kbid=kbid, md5=md5)
    if not exists:
        return Response(status_code=404)
    return Response(status_code=200)
