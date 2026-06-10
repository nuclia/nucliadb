# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import uuid

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_utils.storages.azure import AzureStorage
from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.storages.s3 import S3Storage
from nucliadb_utils.storages.storage import KB_RESOURCE_FIELD, Storage, StorageField


async def test_s3_driver(s3_storage: S3Storage):
    await storage_field_test(s3_storage)


async def test_gcs_driver(gcs_storage: GCSStorage):
    await storage_field_test(gcs_storage)


async def test_local_driver(local_storage: LocalStorage):
    await storage_field_test(local_storage)


async def test_azure_driver(azure_storage: AzureStorage):
    await storage_field_test(azure_storage)


async def storage_field_test(storage: Storage):
    binary_data = b"mytestinfo"
    kbid = uuid.uuid4().hex
    assert await storage.create_kb(kbid)
    bucket = storage.get_bucket_name(kbid)

    # Upload bytes to a key pointing to a file field
    rid = "rid"
    field_id = "field1"
    field_key = KB_RESOURCE_FIELD.format(kbid=kbid, uuid=rid, field=field_id)
    await storage.chunked_upload_object(
        bucket, field_key, binary_data, filename="myfile.txt", content_type="text/plain"
    )

    # Get the storage field object
    sfield: StorageField = storage.file_field(kbid, rid, field=field_id)

    # Check that object's metadata is stored properly
    metadata = await sfield.exists()
    assert metadata is not None
    assert metadata.content_type == "text/plain"
    assert metadata.size == len(binary_data)
    assert metadata.filename == "myfile.txt"

    # Download the file and check that it's the same
    async def check_downloaded_data(sfield, expected_data: bytes):
        downloaded_data = b""
        try:
            async for data in sfield.iter_data():
                downloaded_data += data
        except KeyError:
            # The file does not exist
            pass
        assert downloaded_data == expected_data

    await check_downloaded_data(sfield, binary_data)

    # Test
    if storage.source == CloudFile.Source.LOCAL:
        # There is a bug to be fixed in the copy method on the local storage driver
        return

    # Copy the file to another bucket (with the same key)
    kbid2 = uuid.uuid4().hex
    assert await storage.create_kb(kbid2)
    bucket2 = storage.get_bucket_name(kbid2)
    field_key = KB_RESOURCE_FIELD.format(kbid=kbid2, uuid=rid, field=field_id)
    sfield_kb2 = storage.file_field(kbid2, rid, field=field_id)

    await sfield.copy(sfield.key, field_key, bucket, bucket2)

    await check_downloaded_data(sfield_kb2, binary_data)
    # Check that the old key is still there
    await check_downloaded_data(sfield, binary_data)

    # Move the file to another key (same bucket)
    new_field_id = "field3"
    new_field_key = KB_RESOURCE_FIELD.format(kbid=kbid2, uuid=rid, field=new_field_id)
    new_sfield = storage.file_field(kbid2, rid, field=new_field_id)

    await sfield_kb2.move(sfield_kb2.key, new_field_key, bucket2, bucket2)

    await check_downloaded_data(new_sfield, binary_data)
    # Check that the old key is empty
    await check_downloaded_data(sfield_kb2, b"")

    # Test upload + download of file with special characters in name
    # Upload bytes to a key pointing to a file field
    rid = "rid"
    field_id = "field_weird"
    field_key = KB_RESOURCE_FIELD.format(kbid=kbid, uuid=rid, field=field_id)
    filename = "weird characters \n áñæ€普通話.txt"
    await storage.chunked_upload_object(
        bucket, field_key, binary_data, filename=filename, content_type="text/plain"
    )

    sfield: StorageField = storage.file_field(kbid, rid, field=field_id)
    metadata = await sfield.exists()
    assert metadata.filename == filename  # type: ignore[ty:unresolved-attribute]
    await check_downloaded_data(sfield, binary_data)
