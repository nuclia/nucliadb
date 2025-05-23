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

from math import ceil
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

import pytest
from nidx_protos.noderesources_pb2 import Resource as BrainResource
from nidx_protos.noderesources_pb2 import ResourceID
from nidx_protos.nodewriter_pb2 import IndexMessage

from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_utils.storages.local import LocalStorageField
from nucliadb_utils.storages.storage import (
    ObjectInfo,
    Storage,
    StorageField,
    iter_and_add_size,
    iter_in_chunk_size,
)


class TestStorageField:
    @pytest.fixture
    def storage(self):
        yield AsyncMock(source=0)

    @pytest.fixture
    def field(self):
        yield MagicMock(uri="uri")

    @pytest.fixture
    def storage_field(self, storage, field):
        yield LocalStorageField(storage, "bucket", "fullkey", field)

    async def test_delete(self, storage_field: StorageField, storage):
        await storage_field.delete()
        storage.delete_upload.assert_called_once_with("uri", "bucket")


class StorageTest(Storage):
    def __init__(self):
        self.source = 0
        self.field_klass = lambda: MagicMock()
        self.deadletter_bucket = "deadletter_bucket"
        self.indexing_bucket = "indexing_bucket"
        self.delete_upload = AsyncMock()
        self.chunked_upload_object = AsyncMock()
        self.upload_object = AsyncMock()
        self.move = AsyncMock()

    def get_bucket_name(self, kbid):
        return "bucket"

    async def iterate_objects(self, bucket_name, prefix, start: Optional[str] = None):
        yield ObjectInfo(name="uri")

    async def download(self, bucket_name, uri):
        br = BrainResource(labels=["label"])
        yield br.SerializeToString()

    async def create_kb(self, kbid):
        return True

    async def delete_kb(self, kbid):
        return True

    async def delete_upload(self, uri, bucket):
        return True

    async def initialize(self) -> None:
        pass

    async def finalize(self) -> None:
        pass

    async def schedule_delete_kb(self, kbid: str) -> bool:
        return True

    async def insert_object(self, bucket, key, data):
        pass

    async def create_bucket(self, bucket_name):
        pass


class TestStorage:
    @pytest.fixture
    def storage(self):
        yield StorageTest()

    async def test_delete_resource(self, storage: StorageTest):
        await storage.delete_resource("bucket", "uri")
        storage.delete_upload.assert_called_once_with("uri", "bucket")

    async def test_indexing(self, storage: StorageTest):
        msg = BrainResource(resource=ResourceID(uuid="uuid"))
        await storage.indexing(msg, 1, "1", "kb", "shard")

        storage.upload_object.assert_called_once_with(
            "indexing_bucket", "index/kb/shard/uuid/1", msg.SerializeToString()
        )

    async def test_reindexing(self, storage: StorageTest):
        msg = BrainResource(resource=ResourceID(uuid="uuid"))
        await storage.reindexing(msg, "reindex_id", "1", "kb", "shard")

        storage.upload_object.assert_called_once_with(
            "indexing_bucket", "index/kb/shard/uuid/reindex_id", msg.SerializeToString()
        )

    async def test_get_indexing(self, storage: StorageTest):
        im = IndexMessage()
        im.node = "node"
        im.shard = "shard"
        im.txid = 0
        assert isinstance(await storage.get_indexing(im), BrainResource)

    async def test_get_indexing_storage_key(self, storage: StorageTest):
        im = IndexMessage()
        im.node = "node"
        im.shard = "shard"
        im.txid = 0
        im.storage_key = "index/kb/uuid/1"
        assert isinstance(await storage.get_indexing(im), BrainResource)

    async def test_delete_indexing(self, storage: StorageTest):
        im = IndexMessage()
        im.node = "node"
        im.txid = 0
        im.storage_key = "index/kb/uuid/1"
        await storage.delete_indexing(
            resource_uid="resource_uid", txid=1, kb="kb", logical_shard="logical_shard"
        )

        storage.upload_object.assert_called_once()

    async def test_download_pb(self, storage: StorageTest):
        assert isinstance(
            await storage.download_pb(LocalStorageField(storage, "bucket", "fullkey"), BrainResource),
            BrainResource,
        )

    async def test_indexing_bucket_none_attributeerrror(self, storage: StorageTest):
        storage.indexing_bucket = None
        msg = BrainResource()
        im = IndexMessage(node="node", shard="shard", txid=0)

        with pytest.raises(AttributeError):
            await storage.indexing(msg, 1, "1", "kb", "shard")

        with pytest.raises(AttributeError):
            await storage.reindexing(msg, "reindex_id", "1", "kb", "shard")

        with pytest.raises(AttributeError):
            await storage.get_indexing(im)

        with pytest.raises(AttributeError):
            await storage.delete_indexing(
                resource_uid="resource_uid",
                txid=1,
                kb="kb",
                logical_shard="logical_shard",
            )


async def testiter_and_add_size():
    cf = CloudFile()

    async def iter():
        yield b"foo"
        yield b"bar"

    cf.size = 0
    async for _ in iter_and_add_size(iter(), cf):
        pass

    assert cf.size == 6


async def test_iter_in_chunk_size():
    async def iterable(total_size, *, chunk_size=1):
        data = b"0" * total_size
        for i in range(ceil(total_size / chunk_size)):
            chunk = data[i * chunk_size : (i + 1) * chunk_size]
            yield chunk

    chunks = [chunk async for chunk in iter_in_chunk_size(iterable(10), chunk_size=4)]
    assert len(chunks) == 3
    assert len(chunks[0]) == 4
    assert len(chunks[1]) == 4
    assert len(chunks[2]) == 2

    chunks = [chunk async for chunk in iter_in_chunk_size(iterable(0), chunk_size=4)]
    assert len(chunks) == 0

    # Try with an iterable that yields chunks bigger than the chunk size
    chunks = [
        chunk async for chunk in iter_in_chunk_size(iterable(total_size=12, chunk_size=10), chunk_size=4)
    ]
    assert len(chunks) == 3
    assert len(chunks[0]) == 4
    assert len(chunks[1]) == 4
    assert len(chunks[2]) == 4
