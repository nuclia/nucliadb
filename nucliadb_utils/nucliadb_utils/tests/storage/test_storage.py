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
    key = "mytest"
    kbid = "mykb"
    created = await storage.create_kb(kbid)
    assert created

    bucket = storage.get_bucket_name(kbid)

    await storage.uploadbytes(bucket, key, example)

    async for data in storage.download(bucket, key):
        assert data == example

    metadata = await storage.get_custom_metadata(bucket, key)
    assert metadata != {}
    assert "Foo" not in metadata
    await storage.set_custom_metadata(bucket, key, metadata={"Foo": "Bar"})

    assert (await storage.get_custom_metadata(bucket, key))["Foo"] == "Bar"

    await storage.create_object(bucket, "mytest-other")

    keys_found = 0
    async for keys in storage.iterate_bucket(bucket, ""):
        keys_found += 1
        assert keys["name"].startswith("mytest")
    assert keys_found == 2

    deleted = await storage.schedule_delete_kb(kbid)
    assert deleted
