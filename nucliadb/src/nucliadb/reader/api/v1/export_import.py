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
from typing import AsyncGenerator, AsyncIterable, Union

from fastapi.responses import StreamingResponse
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.common import datamanagers
from nucliadb.common.cluster.settings import in_standalone_mode
from nucliadb.common.context import ApplicationContext
from nucliadb.common.context.fastapi import get_app_context
from nucliadb.export_import import exceptions as export_exceptions
from nucliadb.export_import import exporter
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.exceptions import MetadataNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.reader.api.v1.router import KB_PREFIX, api
from nucliadb_models.export_import import Status, StatusResponse
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires_one


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/export/{{export_id}}",
    status_code=200,
    summary="Download a Knowledge Box export",
    tags=["Knowledge Boxes"],
    response_class=StreamingResponse,
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.READER])
@version(1)
async def download_export_kb_endpoint(request: Request, kbid: str, export_id: str):
    context = get_app_context(request.app)
    if not await exists_kb(kbid):
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")

    if in_standalone_mode():
        # In standalone mode, we stream the export as we generate it.
        return StreamingResponse(
            exporter.export_kb(context, kbid),
            status_code=200,
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={kbid}.export"},
        )
    try:
        return StreamingResponse(
            await download_export_from_blob_storage(context, kbid, export_id),
            status_code=200,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f"attachment; filename={kbid}.export",
            },
        )
    except export_exceptions.MetadataNotFound:
        return HTTPClientError(status_code=404, detail="Export not found")
    except export_exceptions.TaskNotFinishedError:
        return HTTPClientError(
            status_code=412,
            detail=f"Export not yet finished",
        )
    except export_exceptions.TaskErrored:
        return HTTPClientError(status_code=500, detail=f"Export errored")


async def download_export_from_blob_storage(
    context: ApplicationContext, kbid: str, export_id: str
) -> AsyncIterable[bytes]:
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
    metadata = await dm.get_metadata("export", kbid, export_id)
    export_exceptions.raise_for_task_status(metadata.task.status)
    return download_export_and_delete(dm, kbid, export_id)


async def download_export_and_delete(
    dm: ExportImportDataManager, kbid: str, export_id: str
) -> AsyncGenerator[bytes, None]:
    async for chunk in dm.download_export(kbid, export_id):
        yield chunk
    await dm.try_delete_from_storage("export", kbid, export_id)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/export/{{export_id}}/status",
    status_code=200,
    summary="Get the status of a Knowledge Box Export",
    response_model=StatusResponse,
    tags=["Knowledge Boxes"],
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.READER])
@version(1)
async def get_export_status_endpoint(
    request: Request, kbid: str, export_id: str
) -> Union[StatusResponse, HTTPClientError]:
    context = get_app_context(request.app)
    if not await exists_kb(kbid):
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")

    return await _get_status(context, "export", kbid, export_id)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/import/{{import_id}}/status",
    status_code=200,
    summary="Get the status of a Knowledge Box Import",
    response_model=StatusResponse,
    tags=["Knowledge Boxes"],
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.READER])
@version(1)
async def get_import_status_endpoint(
    request: Request, kbid: str, import_id: str
) -> Union[StatusResponse, HTTPClientError]:
    context = get_app_context(request.app)
    if not await exists_kb(kbid):
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")

    return await _get_status(context, "import", kbid, import_id)


async def _get_status(
    context: ApplicationContext, type: str, kbid: str, id: str
) -> Union[StatusResponse, HTTPClientError]:
    if type not in ("export", "import"):
        raise ValueError(f"Incorrect type: {type}")

    if in_standalone_mode():
        # In standalone mode exports/imports are not actually run in a background task.
        # We return always FINISHED status to keep the API compatible with the hosted mode.
        return StatusResponse(status=Status.FINISHED)

    try:
        dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
        metadata = await dm.get_metadata(type, kbid, id)
        return StatusResponse(
            status=metadata.task.status,
            total=metadata.total,
            processed=metadata.processed,
            retries=metadata.task.retries,
        )
    except MetadataNotFound:
        return HTTPClientError(status_code=404, detail=f"{type.capitalize()} not found")


async def exists_kb(kbid: str) -> bool:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.kb.exists_kb(txn, kbid=kbid)
