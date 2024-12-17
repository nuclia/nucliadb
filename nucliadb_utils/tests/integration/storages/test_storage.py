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

from nucliadb_utils.storages.azure import AzureStorage
from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.storages.s3 import S3Storage
from nucliadb_utils.storages.storage import Storage


async def test_azure_driver(azure_storage: AzureStorage):
    assert isinstance(azure_storage, AzureStorage)
    await storage_test(azure_storage)


async def test_s3_driver(s3_storage: S3Storage):
    assert isinstance(s3_storage, S3Storage)
    await storage_test(s3_storage)


async def test_gcs_driver(gcs_storage: GCSStorage):
    assert isinstance(gcs_storage, GCSStorage)
    await storage_test(gcs_storage)


async def test_local_driver(local_storage: LocalStorage):
    assert isinstance(local_storage, LocalStorage)
    await storage_test(local_storage)


async def storage_test(storage: Storage):
    kbid1 = uuid4().hex
    kbid2 = uuid4().hex

    assert await storage.create_kb(kbid1)
    assert await storage.create_kb(kbid2)

    bucket = storage.get_bucket_name(kbid1)

    # Upload a small file
    key0 = "small"
    smallfile = b"mytestinfo"
    await storage.uploadbytes(bucket, key0, smallfile)
    downloaded_data = b""
    async for chunk in storage.download(bucket, key0):
        downloaded_data += chunk
    assert downloaded_data == smallfile

    # Now delete the file
    await storage.delete_upload(key0, bucket)

    # Add a file with size larger than the chunk size
    bigfile = b"mytestinfo" * storage.chunk_size
    key1 = "mytest1"
    key2 = "mytest2"
    await storage.uploadbytes(bucket, key1, bigfile)
    await storage.uploadbytes(bucket, key2, bigfile)

    # Check that the file is there and has the right data
    for key in (key1, key2):
        downloaded_data = b""
        async for chunk in storage.download(bucket, key):
            downloaded_data += chunk
        assert downloaded_data == bigfile

    # Delete one of the keys
    await storage.delete_upload(key2, bucket)

    # Check that the downloaded key is not there
    names = []
    async for object_info in storage.iterate_objects(bucket, ""):
        names.append(object_info.name)
    assert names == [key1]

    # Check insert object
    key = "barbafoo"
    data = b"lorem ipsum"
    await storage.insert_object(bucket, key, data)

    # Check that the inserted object is there and has the right data
    downloaded_data = b""
    async for chunk in storage.download(bucket, key):
        downloaded_data += chunk
    assert downloaded_data == data

    deleted = await storage.schedule_delete_kb(kbid1)
    assert deleted

    await storage.delete_kb(kbid2)
