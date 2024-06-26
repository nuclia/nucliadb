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
from typing import AsyncGenerator
from uuid import uuid4

from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.common import datamanagers
from nucliadb.common.cluster.settings import in_standalone_mode
from nucliadb.common.context import ApplicationContext
from nucliadb.common.context.fastapi import get_app_context
from nucliadb.export_import import importer
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.exceptions import IncompatibleExport
from nucliadb.export_import.models import (
    ExportMetadata,
    ImportMetadata,
    NatsTaskMessage,
)
from nucliadb.export_import.tasks import get_exports_producer, get_imports_producer
from nucliadb.export_import.utils import stream_compatible_with_kb
from nucliadb.models.responses import HTTPClientError
from nucliadb.writer import logger
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb.writer.back_pressure import maybe_back_pressure
from nucliadb_models.export_import import (
    CreateExportResponse,
    CreateImportResponse,
    Status,
)
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_telemetry import errors
from nucliadb_utils.authentication import requires_one


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/export",
    status_code=200,
    summary="Start an export of a Knowledge Box",
    tags=["Knowledge Boxes"],
    response_model=CreateExportResponse,
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def start_kb_export_endpoint(request: Request, kbid: str):
    context = get_app_context(request.app)
    if not await datamanagers.atomic.kb.exists_kb(kbid=kbid):
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")

    export_id = uuid4().hex
    if in_standalone_mode():
        # In standalone mode, exports are generated at download time.
        # We simply return an export_id to keep the API consistent with hosted nucliadb.
        return CreateExportResponse(export_id=export_id)
    else:
        await start_export_task(context, kbid, export_id)
        return CreateExportResponse(export_id=export_id)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/import",
    status_code=200,
    summary="Start an import to a Knowledge Box",
    tags=["Knowledge Boxes"],
    response_model=CreateImportResponse,
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def start_kb_import_endpoint(request: Request, kbid: str):
    context = get_app_context(request.app)
    if not await datamanagers.atomic.kb.exists_kb(kbid=kbid):
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")

    await maybe_back_pressure(request, kbid)

    stream = stream_compatible_with_kb(kbid, request.stream())
    try:
        import_id = uuid4().hex
        if in_standalone_mode():
            # In standalone mode, we import directly from the request content stream.
            # Note that we return an import_id simply to keep the API consistent with hosted nucliadb.
            await importer.import_kb(
                context=context,
                kbid=kbid,
                stream=stream,
            )
            return CreateImportResponse(import_id=import_id)
        else:
            import_size = await upload_import_to_blob_storage(
                context=context,
                stream=stream,
                kbid=kbid,
                import_id=import_id,
            )
            await start_import_task(context, kbid, import_id, import_size)
            return CreateImportResponse(import_id=import_id)
    except IncompatibleExport as exc:
        return HTTPClientError(status_code=400, detail=str(exc))


async def upload_import_to_blob_storage(
    context: ApplicationContext,
    stream: AsyncGenerator[bytes, None],
    kbid: str,
    import_id: str,
) -> int:
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
    return await dm.upload_import(
        import_bytes=stream,
        kbid=kbid,
        import_id=import_id,
    )


async def start_export_task(context: ApplicationContext, kbid: str, export_id: str):
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
    metadata = ExportMetadata(kbid=kbid, id=export_id)
    metadata.task.status = Status.SCHEDULED
    await dm.set_metadata("export", metadata)
    try:
        producer = await get_exports_producer(context)
        msg = NatsTaskMessage(kbid=kbid, id=export_id)
        seqid = await producer(msg)  # type: ignore
        logger.info(f"Export task produced. seqid={seqid} kbid={kbid} export_id={export_id}")
    except Exception as e:
        errors.capture_exception(e)
        await dm.delete_metadata("export", metadata)
        raise


async def start_import_task(context: ApplicationContext, kbid: str, import_id: str, import_size: int):
    dm = ExportImportDataManager(context.kv_driver, context.blob_storage)
    metadata = ImportMetadata(kbid=kbid, id=import_id)
    metadata.task.status = Status.SCHEDULED
    metadata.total = import_size or 0
    await dm.set_metadata("import", metadata)
    try:
        producer = await get_imports_producer(context)
        msg = NatsTaskMessage(kbid=kbid, id=import_id)
        seqid = await producer(msg)  # type: ignore
        logger.info(f"Import task produced. seqid={seqid} kbid={kbid} import_id={import_id}")
    except Exception as e:
        errors.capture_exception(e)
        await dm.delete_metadata("import", metadata)
        raise
