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
from typing import Dict

import pytest

from nucliadb.writer.settings import settings
from nucliadb.writer.tus import get_dm
from nucliadb.writer.tus.exceptions import CloudFileNotFound
from nucliadb.writer.tus.gcs import GCloudBlobStore, GCloudFileStorageManager
from nucliadb.writer.tus.s3 import S3BlobStore, S3FileStorageManager
from nucliadb.writer.tus.storage import BlobStore, FileStorageManager
from nucliadb_utils.storages.storage import KB_RESOURCE_FIELD


@pytest.mark.asyncio
async def test_s3_driver(s3_storage_tus: S3BlobStore):
    settings.dm_enabled = False
    await storage_test(s3_storage_tus, S3FileStorageManager(s3_storage_tus))
    settings.dm_enabled = True


@pytest.mark.asyncio
async def test_gcs_driver(gcs_storage_tus: GCloudBlobStore):
    settings.dm_enabled = False
    await storage_test(gcs_storage_tus, GCloudFileStorageManager(gcs_storage_tus))
    settings.dm_enabled = True


async def storage_test(storage: BlobStore, file_storage_manager: FileStorageManager):
    example = b"mytestinfo"
    field = "myfield"
    rid = "myrid"
    kbid = "mykb_tus_test"

    metadata: Dict[str, str] = {}
    bucket_name = await storage.get_bucket_name(kbid)
    assert bucket_name in ["test_mykb_tus_test", "test-mykb-tus-test"]

    assert await storage.check_exists(bucket_name) is False

    exists = await storage.create_bucket(bucket_name)
    assert exists is False

    upload_id = uuid.uuid4().hex
    dm = get_dm()
    await dm.load(upload_id)
    await dm.start({})
    await dm.update(
        upload_file_id=f"{upload_id}",
        rid=rid,
        field=field,
        metadata=metadata,
        deferred_length=True,
        offset=0,
        item=None,
    )

    path = KB_RESOURCE_FIELD.format(kbid=kbid, uuid=rid, field=field)
    await file_storage_manager.start(dm, path=path, kbid=kbid)

    async def generate():
        yield example

    size = await file_storage_manager.append(dm, generate(), 0)
    await dm.update(offset=size)
    assert size == len(example)
    await file_storage_manager.finish(dm)

    async for data in file_storage_manager.read_range(path, kbid, 1, size):
        assert data == example[1:]

    await file_storage_manager.delete_upload(path, kbid)

    with pytest.raises(CloudFileNotFound):
        async for data in file_storage_manager.read_range(path, kbid, 1, size):
            assert data == example[1:]
