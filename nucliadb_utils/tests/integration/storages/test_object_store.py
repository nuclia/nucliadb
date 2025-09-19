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
from typing import AsyncGenerator

import pytest

from nucliadb_utils.storages.object_store import ObjectStore
from nucliadb_utils.storages.utils import ObjectMetadata, Range


@pytest.fixture(scope="function")
def azure_object_store(azure_storage):
    yield azure_storage.object_store


async def test_azure(azure_object_store):
    await object_store_test(azure_object_store)


async def object_store_test(object_store: ObjectStore):
    await buckets_test(object_store)
    await objects_crud_test(object_store)
    await objects_upload_download_test(object_store)
    await multipart_upload_test(object_store)


async def buckets_test(object_store: ObjectStore):
    bucket_name = str(uuid.uuid4())
    assert await object_store.bucket_exists(bucket_name) is False
    assert await object_store.bucket_create(bucket_name) is True
    assert await object_store.bucket_exists(bucket_name) is True
    assert await object_store.bucket_create(bucket_name) is False
    assert await object_store.bucket_delete(bucket_name) == (True, False)
    assert await object_store.bucket_exists(bucket_name) is False
    assert await object_store.bucket_delete(bucket_name) == (False, False)
    assert await object_store.bucket_schedule_delete(bucket_name) is None


async def iter_data(data: BytesIO) -> AsyncGenerator[bytes, None]:
    await asyncio.sleep(0)
    while chunk := data.read(1024):
        yield chunk


async def objects_crud_test(object_store: ObjectStore):
    bucket_name = str(uuid.uuid4())
    await object_store.bucket_create(bucket_name)
    # Upload object
    object_data = BytesIO(b"Hello, world!")
    object_key = "folder/file.txt"
    metadata = ObjectMetadata(
        filename="file.txt", content_type="text/plain", size=len(object_data.getvalue())
    )
    await object_store.upload(bucket_name, object_key, object_data.getvalue(), metadata)

    # Check object exists by getting metadata
    object_metadata = await object_store.get_metadata(bucket_name, object_key)
    assert object_metadata is not None
    assert object_metadata.filename == metadata.filename
    assert object_metadata.content_type == metadata.content_type
    assert object_metadata.size == metadata.size

    # Iter objects
    objects = [object_info async for object_info in object_store.iterate(bucket_name, prefix="")]
    assert len(objects) == 1
    assert objects[0].name == object_key

    objects = [object_info async for object_info in object_store.iterate(bucket_name, prefix="folder")]
    assert len(objects) == 1

    objects = [object_info async for object_info in object_store.iterate(bucket_name, prefix="bar")]
    assert len(objects) == 0

    # Copy object
    object_key_copy = "folder/file_copy.txt"
    await object_store.copy(bucket_name, object_key, bucket_name, object_key_copy)
    object_metadata = await object_store.get_metadata(bucket_name, object_key_copy)
    assert object_metadata is not None

    # Move object
    object_key_move = "folder/file_move.txt"
    await object_store.move(bucket_name, object_key_copy, bucket_name, object_key_move)
    with pytest.raises(KeyError):
        await object_store.get_metadata(bucket_name, object_key_copy)

    # Delete object
    await object_store.delete(bucket_name, object_key)
    await object_store.delete(bucket_name, object_key_move)
    with pytest.raises(KeyError):
        await object_store.get_metadata(bucket_name, object_key)

    # Deleting again should raise an error
    with pytest.raises(KeyError):
        await object_store.delete(bucket_name, object_key)

    # Insert object
    object_data = BytesIO(b"Hello, world!")
    object_key = "folder/file.txt"
    await object_store.insert(bucket_name, object_key, object_data.getvalue())

    # Check object exists by downloading it
    assert await object_store.download(bucket_name, object_key) == object_data.getvalue()


async def objects_upload_download_test(object_store: ObjectStore):
    bucket_name = str(uuid.uuid4())
    await object_store.bucket_create(bucket_name)

    # Test that downloading a non-existing object raises an error
    with pytest.raises(KeyError):
        await object_store.download(bucket_name, "foobar")

    with pytest.raises(KeyError):
        async for chunk in object_store.download_stream(bucket_name, "foobar"):
            ...

    # Upload object
    object_data = BytesIO(b"Hello, world!")
    object_key = "folder/file.txt"
    metadata = ObjectMetadata(
        filename="file.txt", content_type="text/plain", size=len(object_data.getvalue())
    )
    await object_store.upload(bucket_name, object_key, object_data.getvalue(), metadata)

    # Download object fully
    assert await object_store.download(bucket_name, object_key) == object_data.getvalue()

    # Download object stream
    downloaded_data = BytesIO()
    async for chunk in object_store.download_stream(bucket_name, object_key):
        downloaded_data.write(chunk)
    assert downloaded_data.getvalue() == object_data.getvalue()

    # Download object stream -- ranges
    downloaded_data = BytesIO()
    async for chunk in object_store.download_stream(bucket_name, object_key, range=Range(end=6)):
        downloaded_data.write(chunk)
    async for chunk in object_store.download_stream(bucket_name, object_key, range=Range(start=7)):
        downloaded_data.write(chunk)
    assert downloaded_data.getvalue() == object_data.getvalue()


async def multipart_upload_test(object_store: ObjectStore):
    bucket_name = str(uuid.uuid4())
    await object_store.bucket_create(bucket_name)

    # Create a file with 10MB of data
    mb = 1024 * 1024
    file = BytesIO(b"0" * 10 * mb)

    # Upload the file in parts
    key = "folder/newfile.txt"
    metadata = ObjectMetadata(filename="file.txt", content_type="text/plain", size=len(file.getvalue()))

    async def file_iterable():
        chunk_size = 3 * mb
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            yield chunk

    # Start, append and finish
    await object_store.upload_multipart_start(bucket_name, key, metadata)
    await object_store.upload_multipart_append(bucket_name, key, file_iterable())
    await object_store.upload_multipart_finish(bucket_name, key)

    # Download the file
    downloaded_data = BytesIO()
    downloaded_data.write(await object_store.download(bucket_name, key))
    assert downloaded_data.getvalue() == file.getvalue()
