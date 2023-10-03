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
import uuid

import pytest

from nucliadb.export_import.datamanager import ExportImportDataManager


@pytest.fixture(scope="function")
def datamanager(maindb_driver, gcs_storage):
    return ExportImportDataManager(maindb_driver, gcs_storage)


EXPORT = b"some-export-bytes"
IMPORT = b"some-import-bytes"


@pytest.fixture(scope="function")
async def kbid_with_bucket(gcs_storage):
    kbid = uuid.uuid4().hex

    bucket_name = gcs_storage.get_bucket_name(kbid)
    await gcs_storage.create_bucket(bucket_name)

    yield kbid

    await gcs_storage.delete_kb(kbid)


async def test_export_upload_and_download(datamanager, kbid_with_bucket):
    kbid = kbid_with_bucket
    export_id = "foo"

    async def iter_bytes(data):
        yield data

    await datamanager.upload_export(iter_bytes(EXPORT), kbid, export_id)

    downloaded = b""
    async for chunk in datamanager.download_export(kbid, export_id):
        downloaded += chunk

    assert downloaded == EXPORT


async def test_import_upload_and_download(datamanager, kbid_with_bucket):
    kbid = kbid_with_bucket
    import_id = "foo"

    async def iter_bytes(data):
        yield data

    await datamanager.upload_import(iter_bytes(IMPORT), kbid, import_id)

    downloaded = b""
    async for chunk in datamanager.download_import(kbid, import_id):
        downloaded += chunk

    assert downloaded == IMPORT
