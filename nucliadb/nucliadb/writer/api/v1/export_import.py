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
from uuid import uuid4

from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.common.context.fastapi import get_app_context
from nucliadb.export_import import importer
from nucliadb.export_import.utils import IteratorExportStream
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb_models.export_import import CreateExportResponse, CreateImportResponse
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires_one


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/export",
    status_code=200,
    name="Start an export of a Knowledge Box",
    tags=["Knowledge Boxes"],
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def start_kb_export_endpoint(request: Request, kbid: str) -> CreateExportResponse:
    export_id = uuid4().hex
    response = CreateExportResponse(export_id=export_id)
    return response


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/import",
    status_code=200,
    name="Start an import to a Knowledge Box",
    tags=["Knowledge Boxes"],
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def start_kb_import_endpoint(request: Request, kbid: str) -> CreateImportResponse:
    context = get_app_context(request.app)
    import_id = uuid4().hex
    stream = FastAPIExportStream(request)
    await importer.import_kb(
        context=context,
        kbid=kbid,
        stream=stream,
    )
    return CreateImportResponse(import_id=import_id)


class FastAPIExportStream(IteratorExportStream):
    def __init__(self, request: Request):
        iterator = request.stream().__aiter__()
        super().__init__(iterator)
