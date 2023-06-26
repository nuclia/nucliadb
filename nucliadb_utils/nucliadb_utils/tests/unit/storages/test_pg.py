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
from unittest.mock import ANY, AsyncMock, MagicMock, call, patch

import pytest
from nucliadb_protos.resources_pb2 import CloudFile

from nucliadb_utils.storages import pg

pytestmark = pytest.mark.asyncio


async def iter_result(data):
    for item in data:
        yield item


@pytest.fixture
def transaction():
    yield MagicMock(return_value=AsyncMock())


@pytest.fixture
def connection(transaction):
    mock = AsyncMock()
    mock.transaction = MagicMock(return_value=transaction)
    yield mock


@pytest.fixture
def pool(connection):
    acquire = MagicMock(return_value=AsyncMock())
    acquire.return_value.__aenter__.return_value = connection
    pool = AsyncMock()
    pool.acquire = acquire
    with patch(
        "nucliadb_utils.storages.pg.asyncpg.create_pool", AsyncMock(return_value=pool)
    ):
        yield pool


@pytest.fixture
def data_layer(connection):
    yield pg.PostgresFileDataLayer(connection)


@pytest.fixture
def storage(data_layer, pool):
    with patch(
        "nucliadb_utils.storages.pg.PostgresFileDataLayer", return_value=data_layer
    ):
        storage = pg.PostgresStorage("dsn")
        storage.pool = pool
        yield storage


@pytest.fixture
def storage_field(storage):
    yield pg.PostgresStorageField(storage, "bucket", "fullkey")


@pytest.fixture
def chunk_info():
    yield [
        {
            "filename": "filename",
            "size": 5,
            "content_type": "content_type",
            "part_id": 0,
        },
        {
            "filename": "filename",
            "size": 5,
            "content_type": "content_type",
            "part_id": 1,
        },
        {
            "filename": "filename",
            "size": 5,
            "content_type": "content_type",
            "part_id": 2,
        },
    ]


@pytest.fixture
def chunk_data():
    yield [
        {
            "part_id": 0,
            "size": 5,
            "data": b"data1",
        },
        {
            "part_id": 1,
            "size": 5,
            "data": b"data2",
        },
        {
            "part_id": 2,
            "size": 5,
            "data": b"data3",
        },
    ]


class TestPostgresFileDataLayer:
    async def test_delete_kb(self, data_layer: pg.PostgresFileDataLayer, connection):
        assert await data_layer.delete_kb("test_kb")

        assert connection.execute.call_count == 2

        connection.execute.assert_has_awaits(
            [
                call(ANY, "test_kb"),
                call(ANY, "test_kb"),
            ]
        )

    async def test_create_file(self, data_layer: pg.PostgresFileDataLayer, connection):
        await data_layer.create_file(
            kb_id="kb_id",
            file_id="file_id",
            filename="filename",
            size=1,
            content_type="content_type",
        )

        connection.execute.assert_awaited_once_with(
            ANY,
            "kb_id",
            "file_id",
            "filename",
            1,
            "content_type",
        )

    async def test_delete_file(self, data_layer: pg.PostgresFileDataLayer, connection):
        await data_layer.delete_file("test_kb", "test_file")

        connection.execute.assert_awaited_with(ANY, "test_kb", "test_file")

    async def test_append_chunk(self, data_layer: pg.PostgresFileDataLayer, connection):
        await data_layer.append_chunk(kb_id="kb_id", file_id="file_id", data=b"data")

        connection.execute.assert_awaited_once_with(ANY, "kb_id", "file_id", b"data", 4)

    async def test_get_file_info(
        self, data_layer: pg.PostgresFileDataLayer, connection
    ):
        record = {
            "filename": "filename",
            "size": 1,
            "content_type": "content_type",
            "file_id": "file_id",
        }
        connection.fetchrow.return_value = record
        assert await data_layer.get_file_info(
            kb_id="kb_id", file_id="file_id"
        ) == pg.FileInfo(
            filename=record["filename"],  # type: ignore
            size=record["size"],  # type: ignore
            content_type=record["content_type"],  # type: ignore
            key=record["file_id"],  # type: ignore
        )

        connection.fetchrow.assert_awaited_once_with(ANY, "kb_id", "file_id")

    async def test_get_file_info_none(
        self, data_layer: pg.PostgresFileDataLayer, connection
    ):
        connection.fetchrow.return_value = None
        assert await data_layer.get_file_info(kb_id="kb_id", file_id="file_id") is None

    async def test_move(self, data_layer: pg.PostgresFileDataLayer, connection):
        await data_layer.move(
            origin_key="origin_key",
            destination_key="destination_key",
            origin_kb="origin_kb",
            destination_kb="destination_kb",
        )

        assert connection.execute.call_count == 4

        connection.execute.assert_has_awaits(
            [
                call(ANY, "destination_kb", "destination_key"),
                call(
                    ANY, "destination_kb", "destination_key", "origin_kb", "origin_key"
                ),
                call(ANY, "destination_kb", "destination_key"),
                call(
                    ANY, "destination_kb", "destination_key", "origin_kb", "origin_key"
                ),
            ]
        )

    async def test_copy(self, data_layer: pg.PostgresFileDataLayer, connection):
        await data_layer.copy(
            origin_key="origin_key",
            destination_key="destination_key",
            origin_kb="origin_kb",
            destination_kb="destination_kb",
        )

        assert connection.execute.call_count == 2

        connection.execute.assert_has_awaits(
            [
                call(
                    ANY, "destination_kb", "destination_key", "origin_kb", "origin_key"
                ),
                call(
                    ANY, "destination_kb", "destination_key", "origin_kb", "origin_key"
                ),
            ]
        )

    async def test_iterate_kb(self, data_layer: pg.PostgresFileDataLayer, connection):
        connection.cursor = MagicMock(
            return_value=iter_result(
                [
                    {
                        "file_id": "file_id",
                        "filename": "filename",
                        "size": 1,
                        "content_type": "content_type",
                    },
                    {
                        "file_id": "file_id",
                        "filename": "filename",
                        "size": 1,
                        "content_type": "content_type",
                    },
                ]
            )
        )

        async for file_info in data_layer.iterate_kb("kb_id", "prefix"):
            assert file_info == pg.FileInfo(
                filename="filename",
                size=1,
                content_type="content_type",
                key="file_id",
            )

        connection.cursor.assert_called_once_with(ANY, "kb_id", "prefix%")

    async def test_iterate_range(
        self, data_layer: pg.PostgresFileDataLayer, connection, chunk_info, chunk_data
    ):
        connection.fetch.return_value = chunk_info
        connection.fetchrow.side_effect = chunk_data

        chunks = []
        async for chunk in data_layer.iterate_range(
            kb_id="kb_id", file_id="file_id", start=3, end=8
        ):
            chunks.append(chunk)

        assert chunks == [b"a1", b"dat"]

    async def test_iterate_range_start_part(
        self, data_layer: pg.PostgresFileDataLayer, connection, chunk_info, chunk_data
    ):
        connection.fetch.return_value = chunk_info
        connection.fetchrow.side_effect = chunk_data

        chunks = []
        async for chunk in data_layer.iterate_range(
            kb_id="kb_id", file_id="file_id", start=0, end=5
        ):
            chunks.append(chunk)

        assert chunks == [b"data1"]

    async def test_iterate_range_middle_part(
        self, data_layer: pg.PostgresFileDataLayer, connection, chunk_info, chunk_data
    ):
        connection.fetch.return_value = chunk_info
        connection.fetchrow.side_effect = chunk_data[1:]

        chunks = []
        async for chunk in data_layer.iterate_range(
            kb_id="kb_id", file_id="file_id", start=5, end=10
        ):
            chunks.append(chunk)

        assert chunks == [b"data2"]

    async def test_iterate_range_end_part(
        self, data_layer: pg.PostgresFileDataLayer, connection, chunk_info, chunk_data
    ):
        connection.fetch.return_value = chunk_info
        connection.fetchrow.side_effect = chunk_data[2:]

        chunks = []
        async for chunk in data_layer.iterate_range(
            kb_id="kb_id", file_id="file_id", start=10, end=15
        ):
            chunks.append(chunk)

        assert chunks == [b"data3"]

    async def test_iterate_range_cross_all(
        self, data_layer: pg.PostgresFileDataLayer, connection, chunk_info, chunk_data
    ):
        connection.fetch.return_value = chunk_info
        connection.fetchrow.side_effect = chunk_data

        chunks = []
        async for chunk in data_layer.iterate_range(
            kb_id="kb_id", file_id="file_id", start=2, end=13
        ):
            chunks.append(chunk)

        assert chunks == [b"ta1", b"data2", b"dat"]


class TestPostgresStorageField:
    @pytest.fixture()
    def field(self):
        yield CloudFile(uri="uri", bucket_name="bucket_name")

    async def test_move(self, storage_field: pg.PostgresStorageField, connection):
        await storage_field.move(
            "origin_uri",
            "destination_uri",
            "origin_bucket_name",
            "destination_bucket_name",
        )

        assert connection.execute.call_count == 4

        connection.execute.assert_has_awaits(
            [
                call(ANY, "destination_bucket_name", "destination_uri"),
                call(
                    ANY,
                    "destination_bucket_name",
                    "destination_uri",
                    "origin_bucket_name",
                    "origin_uri",
                ),
                call(ANY, "destination_bucket_name", "destination_uri"),
                call(
                    ANY,
                    "destination_bucket_name",
                    "destination_uri",
                    "origin_bucket_name",
                    "origin_uri",
                ),
            ]
        )

    async def test_copy(self, storage_field: pg.PostgresStorageField, connection):
        await storage_field.copy(
            "origin_uri",
            "destination_uri",
            "origin_bucket_name",
            "destination_bucket_name",
        )

        assert connection.execute.call_count == 2

        connection.execute.assert_has_awaits(
            [
                call(
                    ANY,
                    "destination_bucket_name",
                    "destination_uri",
                    "origin_bucket_name",
                    "origin_uri",
                ),
                call(
                    ANY,
                    "destination_bucket_name",
                    "destination_uri",
                    "origin_bucket_name",
                    "origin_uri",
                ),
            ]
        )

    async def test_iter_data(
        self,
        storage_field: pg.PostgresStorageField,
        connection,
        chunk_info,
        chunk_data,
        field,
    ):
        storage_field.field = field
        connection.fetch.return_value = chunk_info
        connection.fetchrow.side_effect = chunk_data

        chunks = []
        async for chunk in storage_field.iter_data():
            chunks.append(chunk)

        assert chunks == [b"data1", b"data2", b"data3"]

    async def test_read_range(
        self,
        storage_field: pg.PostgresStorageField,
        connection,
        chunk_info,
        chunk_data,
        field,
    ):
        storage_field.field = field
        connection.fetch.return_value = chunk_info
        connection.fetchrow.side_effect = chunk_data

        chunks = []
        async for chunk in storage_field.read_range(0, 15):
            chunks.append(chunk)

        assert chunks == [b"data1", b"data2", b"data3"]

    async def test_start(
        self,
        storage_field: pg.PostgresStorageField,
        connection,
        field,
    ):
        field.upload_uri = "upload_uri"
        storage_field.field = field

        new_field = await storage_field.start(field)
        assert new_field.upload_uri != "upload_uri"

        assert connection.execute.call_count == 3

    async def test_append(
        self,
        storage_field: pg.PostgresStorageField,
        connection,
        field,
    ):
        field.upload_uri = "upload_uri"
        storage_field.field = field

        await storage_field.append(field, iter_result([b"test1", b"test2"]))

        assert field.offset == 10

        assert connection.execute.call_count == 2

    async def test_finish(
        self,
        storage_field: pg.PostgresStorageField,
        connection,
        field,
    ):
        field.upload_uri = "upload_uri"
        field.old_uri = "old_uri"
        storage_field.field = field

        await storage_field.finish()
        assert field.uri == storage_field.key

        assert connection.execute.call_count == 6

    async def test_upload(
        self,
        storage_field: pg.PostgresStorageField,
        connection,
        field,
    ):
        field.upload_uri = "upload_uri"
        storage_field.field = field

        await storage_field.upload(iter_result([b"test1", b"test2"]), field)

        assert connection.execute.call_count == 9


class TestPostgresStorage:
    async def test_initialize(self, storage: pg.PostgresStorage, pool, connection):
        await storage.initialize()

        assert pool.acquire.call_count == 1
        assert connection.execute.call_count == 1

    async def test_finalize(self, storage: pg.PostgresStorage, pool):
        await storage.finalize()

        pool.close.assert_called_once()

    def test_get_bucket_name(self, storage: pg.PostgresStorage):
        assert storage.get_bucket_name("bucket_name") == "bucket_name"

    async def test_create_kb(self, storage: pg.PostgresStorage):
        assert await storage.create_kb("kb_id")

    async def test_delete_kb(self, storage: pg.PostgresStorage, connection):
        assert await storage.delete_kb("kb_id") == (True, False)
        connection.execute.assert_has_awaits(
            [
                call(ANY, "kb_id"),
                call(ANY, "kb_id"),
            ]
        )

    async def test_delete_upload(self, storage: pg.PostgresStorage, connection):
        await storage.delete_upload("file_id", "kb_id")
        connection.execute.assert_awaited_with(ANY, "kb_id", "file_id")

    async def test_iterate_bucket(self, storage: pg.PostgresStorage, connection):
        connection.cursor = MagicMock(
            return_value=iter_result(
                [
                    {
                        "file_id": "file_id1",
                        "filename": "filename",
                        "size": 1,
                        "content_type": "content_type",
                    },
                    {
                        "file_id": "file_id2",
                        "filename": "filename",
                        "size": 1,
                        "content_type": "content_type",
                    },
                ]
            )
        )

        chunks = []
        async for chunk in storage.iterate_bucket("kb_id", "file_id"):
            chunks.append(chunk)

        assert chunks == [{"name": "file_id1"}, {"name": "file_id2"}]

    async def test_download(
        self, storage: pg.PostgresStorage, connection, chunk_info, chunk_data
    ):
        connection.fetch.return_value = chunk_info
        connection.fetchrow.side_effect = chunk_data

        chunks = []
        async for chunk in storage.download("kb_id", "file_id"):
            chunks.append(chunk)

        assert chunks == [b"data1", b"data2", b"data3"]
