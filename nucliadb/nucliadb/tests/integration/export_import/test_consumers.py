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
import asyncio
import uuid
from io import BytesIO

import pytest
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb.common.context import ApplicationContext
from nucliadb.export_import import consumers, producers
from nucliadb.export_import.datamanager import ExportImportDataManager
from nucliadb.export_import.exceptions import MetadataNotFound

pytestmark = pytest.mark.asyncio


async def _wait_for(
    context: ApplicationContext, kbid, id, type="export", max_wait=None
):
    dm = ExportImportDataManager(context.kv_driver)
    finished = False
    max_wait = max_wait or 200
    for _ in range(max_wait):
        try:
            if type == "export":
                md = await dm.get_export_metadata(kbid, id)
            else:
                md = await dm.get_import_metadata(kbid, id)
        except MetadataNotFound:
            await asyncio.sleep(1)
            continue
        st = md.status
        assert st not in ("errored", "failed")
        if st == "running":
            await asyncio.sleep(1)
        elif st == "finished":
            finished = True
            break
    assert finished


async def download_from_blob_storage(context: ApplicationContext, kbid, export_id):
    export = BytesIO()
    bucket = context.blob_storage.get_bucket_name(kbid)
    key = consumers.KB_EXPORTS.format(export_id=export_id)
    field = context.blob_storage.field_klass(
        storage=context.blob_storage,
        bucket=bucket,
        fullkey=key,
    )
    async for chunk in field.iter_data():
        export.write(chunk)
    export.seek(0)
    return export


async def upload_to_blob_storage(context: ApplicationContext, kbid, export_stream):
    import_id = uuid.uuid4().hex
    bucket = context.blob_storage.get_bucket_name(kbid)
    key = consumers.KB_IMPORTS.format(import_id=import_id)
    destination_field = context.blob_storage.field_klass(
        storage=context.blob_storage,
        bucket=bucket,
        fullkey=key,
    )
    cf = CloudFile()
    cf.bucket_name = bucket
    cf.content_type = "binary/octet-stream"
    await context.blob_storage.uploaditerator(export_stream, destination_field, cf)
    return import_id


async def export_kb_async(context, kbid):
    export_id, _ = await producers.start_export(context, kbid)
    await _wait_for(context, kbid, export_id, type="export", max_wait=10)
    export = await download_from_blob_storage(context, kbid, export_id)
    return export


async def import_kb_async(context, kbid, export):
    async def export_stream():
        while True:
            chunk = export.read(1024)
            if not chunk:
                break
            yield chunk

    import_id = await upload_to_blob_storage(context, kbid, export_stream())
    await producers.start_import(context, kbid, import_id)
    await _wait_for(context, kbid, import_id, type="import")


async def test_export_import_via_consumers(context, kbid_to_export, kbid_to_import):
    await consumers.start_exports_consumer(context)
    await consumers.start_imports_consumer(context)

    export1 = await export_kb_async(context, kbid_to_export)
    await import_kb_async(context, kbid_to_import, export1)

    export2 = await export_kb_async(context, kbid_to_import)
    assert export1 == export2
