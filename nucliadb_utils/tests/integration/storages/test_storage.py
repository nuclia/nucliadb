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

import pytest

from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.storages.s3 import S3Storage
from nucliadb_utils.storages.storage import Storage


@pytest.mark.asyncio
async def test_s3_driver(s3_storage: S3Storage):
    await storage_test(s3_storage)


@pytest.mark.asyncio
async def test_gcs_driver(gcs_storage: GCSStorage):
    await storage_test(gcs_storage)


@pytest.mark.asyncio
async def test_local_driver(local_storage: LocalStorage):
    await storage_test(local_storage)


async def storage_test(storage: Storage):
    example = b"mytestinfo"
    key1 = "mytest1"
    key2 = "mytest2"
    kbid1 = uuid4().hex
    kbid2 = uuid4().hex

    assert await storage.create_kb(kbid1)
    assert await storage.create_kb(kbid2)

    bucket = storage.get_bucket_name(kbid1)

    await storage.uploadbytes(bucket, key1, example)
    await storage.uploadbytes(bucket, key2, example)
    async for data in storage.download(bucket, key1):
        assert data == example

    await storage.delete_upload(key2, bucket)

    async for object_info in storage.iterate_objects(bucket, ""):
        assert object_info.name == key1

    deleted = await storage.schedule_delete_kb(kbid1)
    assert deleted

    await storage.delete_kb(kbid2)
