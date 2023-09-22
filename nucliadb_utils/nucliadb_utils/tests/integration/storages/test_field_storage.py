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

from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.storages.s3 import S3Storage
from nucliadb_utils.storages.storage import KB_RESOURCE_FIELD, Storage, StorageField


@pytest.mark.asyncio
async def test_s3_driver(s3_storage: S3Storage):
    await storage_field_test(s3_storage)


@pytest.mark.asyncio
async def test_gcs_driver(gcs_storage: GCSStorage):
    await storage_field_test(gcs_storage)


@pytest.mark.asyncio
async def test_local_driver(local_storage: LocalStorage):
    await storage_field_test(local_storage)


async def storage_field_test(storage: Storage):
    binary_data = b"mytestinfo"
    kbid = uuid.uuid4().hex
    assert await storage.create_kb(kbid)
    bucket = storage.get_bucket_name(kbid)

    # Upload bytes to a key pointing to a file field
    rid = "rid"
    field_id = "field1"
    field_key = KB_RESOURCE_FIELD.format(kbid=kbid, uuid=rid, field=field_id)
    await storage.uploadbytes(
        bucket, field_key, binary_data, filename="myfile.txt", content_type="text/plain"
    )

    # Get the storage field object
    sfield: StorageField = storage.file_field(kbid, rid, field=field_id)

    # Check that object's metadata is stored properly
    metadata = await sfield.exists()
    assert metadata is not None
    assert metadata["CONTENT_TYPE"] == "text/plain"
    assert str(metadata["SIZE"]) == str(len(binary_data))
    assert metadata["FILENAME"] == "myfile.txt"

    # Download the file and check that it's the same
    downloaded_data = b""
    async for data in sfield.iter_data():
        downloaded_data += data
    assert downloaded_data == binary_data
