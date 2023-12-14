import pytest

from nucliadb_utils.storages.pg import PostgresFileDataLayer, PostgresStorage

pytestmark = pytest.mark.asyncio


class TestPostgresFileDataLayer:
    @pytest.fixture()
    async def data_layer(self, pg_storage: PostgresStorage):
        async with pg_storage.pool.acquire() as conn:
            yield PostgresFileDataLayer(conn)

    async def test_move_file(self, data_layer: PostgresFileDataLayer):
        await data_layer.create_file(
            kb_id="kb_id",
            file_id="file_id",
            filename="filename",
            size=5,
            content_type="content_type",
        )
        await data_layer.append_chunk(kb_id="kb_id", file_id="file_id", data=b"12345")
        await data_layer.move(
            origin_key="file_id",
            destination_key="new_file_id",
            origin_kb="kb_id",
            destination_kb="kb_id",
        )

        assert (
            await data_layer.get_file_info(
                kb_id="kb_id",
                file_id="file_id",
            )
            is None
        )
        assert (
            await data_layer.get_file_info(
                kb_id="kb_id",
                file_id="new_file_id",
            )
            is not None
        )

        assert (
            b"".join(
                [
                    chunk["data"]
                    async for chunk in data_layer.iterate_chunks("kb_id", "new_file_id")
                ]
            )
            == b"12345"
        )

    async def test_move_file_overwrites(self, data_layer: PostgresFileDataLayer):
        await data_layer.create_file(
            kb_id="kb_id",
            file_id="file_id1",
            filename="filename",
            size=5,
            content_type="content_type",
        )
        await data_layer.append_chunk(kb_id="kb_id", file_id="file_id1", data=b"12345")
        await data_layer.create_file(
            kb_id="kb_id",
            file_id="file_id2",
            filename="filename",
            size=5,
            content_type="content_type",
        )
        await data_layer.append_chunk(kb_id="kb_id", file_id="file_id2", data=b"67890")

        await data_layer.move(
            origin_key="file_id2",
            destination_key="file_id1",
            origin_kb="kb_id",
            destination_kb="kb_id",
        )

        assert (
            await data_layer.get_file_info(
                kb_id="kb_id",
                file_id="file_id2",
            )
            is None
        )
        assert (
            await data_layer.get_file_info(
                kb_id="kb_id",
                file_id="file_id1",
            )
            is not None
        )

        assert (
            b"".join(
                [
                    chunk["data"]
                    async for chunk in data_layer.iterate_chunks("kb_id", "file_id1")
                ]
            )
            == b"67890"
        )
        assert True
