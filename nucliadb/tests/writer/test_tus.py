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
import tempfile
import uuid

import pytest

from nucliadb.writer.settings import settings
from nucliadb.writer.tus import get_dm
from nucliadb.writer.tus.gcs import GCloudBlobStore, GCloudFileStorageManager
from nucliadb.writer.tus.local import LocalBlobStore, LocalFileStorageManager
from nucliadb.writer.tus.s3 import S3BlobStore, S3FileStorageManager
from nucliadb.writer.tus.storage import BlobStore, FileStorageManager
from nucliadb_utils.storages.storage import KB_RESOURCE_FIELD


@pytest.fixture(scope="function")
async def s3_storage_tus(s3):
    storage = S3BlobStore()
    await storage.initialize(
        client_id="",
        client_secret="",
        max_pool_connections=2,
        endpoint_url=s3,
        verify_ssl=False,
        ssl=False,
        region_name=None,
        bucket="test_{kbid}",
        bucket_tags={"testTag": "test"},
        kms_key_id="test_kms_key_id",
    )
    yield storage
    await storage.finalize()


@pytest.fixture(scope="function")
async def gcs_storage_tus(gcs):
    storage = GCloudBlobStore()
    await storage.initialize(
        json_credentials=None,
        bucket="test_{kbid}",
        location="location",
        project="project",
        bucket_labels={},
        object_base_url=gcs,
    )
    yield storage
    await storage.finalize()


@pytest.fixture(scope="function")
async def local_storage_tus():
    folder = tempfile.TemporaryDirectory()
    storage = LocalBlobStore(local_testing_files=folder.name)
    await storage.initialize()
    yield storage
    await storage.finalize()
    folder.cleanup()


async def clean_dm():
    from nucliadb.writer.tus import REDIS_FILE_DATA_MANAGER_FACTORY

    if REDIS_FILE_DATA_MANAGER_FACTORY is not None:
        await REDIS_FILE_DATA_MANAGER_FACTORY.finalize()
        REDIS_FILE_DATA_MANAGER_FACTORY = None


@pytest.fixture(scope="function")
async def redis_dm(valkey):
    prev = settings.dm_enabled

    settings.dm_enabled = True
    settings.dm_redis_host = valkey[0]
    settings.dm_redis_port = valkey[1]

    dm = get_dm()

    yield dm

    await clean_dm()

    settings.dm_enabled = prev


async def test_s3_driver(redis_dm, s3_storage_tus: S3BlobStore):
    await storage_test(s3_storage_tus, S3FileStorageManager(s3_storage_tus))


async def test_gcs_driver(redis_dm, gcs_storage_tus: GCloudBlobStore):
    await storage_test(gcs_storage_tus, GCloudFileStorageManager(gcs_storage_tus))


async def test_local_driver(local_storage_tus: LocalBlobStore):
    settings.dm_enabled = False
    await storage_test(local_storage_tus, LocalFileStorageManager(local_storage_tus))
    settings.dm_enabled = True


async def storage_test(storage: BlobStore, file_storage_manager: FileStorageManager):
    example = b"mytestinfo"
    field = "myfield"
    rid = "myrid"
    kbid = "mykb_tus_test"

    metadata: dict[str, str] = {"filename": "non-ascii is problematic - Ôñ"}
    bucket_name = storage.get_bucket_name(kbid)
    assert bucket_name in [
        "test_mykb_tus_test",
        "test-mykb-tus-test",
        "ndb_mykb_tus_test",
        "mykb_tus_test",
    ]

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
    await file_storage_manager.delete_upload(path, kbid)
