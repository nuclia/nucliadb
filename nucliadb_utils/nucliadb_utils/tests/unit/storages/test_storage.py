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

from unittest.mock import AsyncMock, MagicMock

import pytest
from nucliadb_protos.noderesources_pb2 import Resource as BrainResource
from nucliadb_protos.nodewriter_pb2 import IndexMessage
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_utils.storages.storage import Storage, StorageField


class TestStorageField:
    @pytest.fixture
    def storage(self):
        yield AsyncMock(source=0)

    @pytest.fixture
    def field(self):
        yield MagicMock(uri="uri")

    @pytest.fixture
    def storage_field(self, storage, field):
        yield StorageField(storage, "bucket", "fullkey", field)

    @pytest.mark.asyncio
    async def test_delete(self, storage_field: StorageField, storage):
        await storage_field.delete()
        storage.delete_upload.assert_called_once_with("bucket", "uri")

    def test_build_cf(self, storage_field: StorageField):
        cf = CloudFile()
        cf.bucket_name = "bucket"
        cf.uri = "fullkey"
        cf.filename = "payload.pb"
        assert storage_field.build_cf() == cf


class StorageTest(Storage):
    def __init__(self):
        self.source = 0
        self.field_klass = lambda: MagicMock()
        self.deadletter_bucket = "deadletter_bucket"
        self.indexing_bucket = "indexing_bucket"
        self.delete_upload = AsyncMock()
        self.uploadbytes = AsyncMock()
        self.move = AsyncMock()

    def get_bucket_name(self, kbid):
        return "bucket"

    async def iterate_bucket(self, bucket_name, prefix):
        yield {"name": "uri"}

    async def download(self, bucket_name, uri):
        br = BrainResource(labels=["label"])
        yield br.SerializeToString()


class TestStorage:
    @pytest.fixture
    def storage(self):
        yield StorageTest()

    @pytest.mark.asyncio
    async def test_delete_resource(self, storage: StorageTest):
        await storage.delete_resource("bucket", "uri")

        storage.delete_upload.assert_called_once_with("uri", "bucket")

    @pytest.mark.asyncio
    async def test_indexing(self, storage: StorageTest):
        msg = BrainResource()
        await storage.indexing(msg, "node", "shard", 1, "1")

        storage.uploadbytes.assert_called_once_with(
            "indexing_bucket", "index/node/shard/1", msg.SerializeToString()
        )

    @pytest.mark.asyncio
    async def test_reindexing(self, storage: StorageTest):
        msg = BrainResource()
        await storage.reindexing(msg, "node", "shard", "reindex_id", "1")

        storage.uploadbytes.assert_called_once_with(
            "indexing_bucket", "index/node/shard/reindex_id", msg.SerializeToString()
        )

    @pytest.mark.asyncio
    async def test_get_indexing(self, storage: StorageTest):
        im = IndexMessage()
        im.node = "node"
        im.shard = "shard"
        im.txid = 0
        assert isinstance(await storage.get_indexing(im), BrainResource)

    @pytest.mark.asyncio
    async def test_delete_indexing(self, storage: StorageTest):
        im = IndexMessage()
        im.node = "node"
        im.shard = "shard"
        im.txid = 0
        await storage.delete_indexing(im)

        storage.delete_upload.assert_called_once()

    @pytest.mark.asyncio
    async def test_download_pb(self, storage: StorageTest):
        assert isinstance(
            await storage.download_pb(
                StorageField(storage, "bucket", "fullkey"), BrainResource
            ),
            BrainResource,
        )

    @pytest.mark.asyncio
    async def test_indexing_bucket_none_attributeerrror(self, storage: StorageTest):
        storage.indexing_bucket = None
        msg = BrainResource()
        im = IndexMessage(node="node", shard="shard", txid=0)

        with pytest.raises(AttributeError):
            await storage.indexing(msg, "node", "shard", 1, "1")

        with pytest.raises(AttributeError):
            await storage.reindexing(msg, "node", "shard", "reindex_id", "1")

        with pytest.raises(AttributeError):
            await storage.get_indexing(im)

        with pytest.raises(AttributeError):
            await storage.delete_indexing(im)
