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
import unittest

import pytest

from nucliadb_utils.aiopynecone.client import (
    ControlPlane,
    DataPlane,
    PineconeSession,
)
from nucliadb_utils.aiopynecone.models import QueryResponse, Vector


async def test_session():
    async with PineconeSession() as session:
        cp1 = session.control_plane(api_key="api_key")
        assert cp1.http_session is session.control_plane_session

        cp2 = session.control_plane(api_key="api_key_2")
        assert cp2.http_session is session.control_plane_session

        dp1 = session.data_plane(api_key="api_key", index_host="index_host", timeout=1)
        assert str(dp1.http_session.base_url) == "https://index_host/"
        assert dp1.client_timeout == 1

        # Check that sessions for the same index host are cached
        dp1_copy = session.data_plane(api_key="api_key", index_host="index_host")
        assert dp1_copy.http_session is dp1.http_session

        # Check that a different data plane client gets a different session
        dp2 = session.data_plane(api_key="api_key_2", index_host="index_host_2")
        assert str(dp2.http_session.base_url) == "https://index_host_2/"
        assert dp2.client_timeout is None
        assert dp2.http_session is not session.control_plane_session
        assert dp2.http_session is not dp1.http_session


class TestControlPlane:
    @pytest.fixture()
    def http_response(self):
        response_mock = unittest.mock.Mock()
        response_mock.raise_for_status.return_value = None
        return response_mock

    @pytest.fixture()
    def http_session(self, http_response):
        session_mock = unittest.mock.Mock()
        session_mock.post = unittest.mock.AsyncMock(return_value=http_response)
        session_mock.delete = unittest.mock.AsyncMock(return_value=http_response)
        return session_mock

    @pytest.fixture()
    def client(self, http_session) -> ControlPlane:
        client = PineconeSession().control_plane(api_key="api_key")
        client.http_session = http_session
        return client

    async def test_create_index(self, client: ControlPlane, http_session, http_response):
        http_response.json.return_value = {"host": "host"}

        host = await client.create_index(name="name", dimension=1)
        assert host == "host"
        http_session.post.assert_called_once()

    async def test_delete_index(self, client: ControlPlane, http_session):
        await client.delete_index(name="name")

        http_session.delete.assert_called_once()


class TestDataPlane:
    @pytest.fixture()
    def http_response(self):
        response_mock = unittest.mock.Mock()
        response_mock.raise_for_status.return_value = None
        return response_mock

    @pytest.fixture()
    def http_session(self, http_response):
        session_mock = unittest.mock.Mock()
        session_mock.post = unittest.mock.AsyncMock(return_value=http_response)
        session_mock.get = unittest.mock.AsyncMock(return_value=http_response)
        return session_mock

    @pytest.fixture()
    def client(self, http_session) -> DataPlane:
        client = PineconeSession().data_plane(api_key="api_key", index_host="index_host")
        client.http_session = http_session
        return client

    async def test_upsert(self, client: DataPlane, http_session):
        await client.upsert(vectors=[Vector(id="id", values=[1.0])])

        http_session.post.assert_called_once()

    async def test_upsert_in_batches(self, client: DataPlane, http_session):
        await client.upsert_in_batches(vectors=[Vector(id="id", values=[1.0])], max_parallel_batches=1)

        http_session.post.assert_called_once()

    async def test_delete(self, client: DataPlane, http_session):
        await client.delete(ids=["id"])

        http_session.post.assert_called_once()

    async def test_query(self, client: DataPlane, http_session, http_response):
        http_response.json.return_value = {
            "matches": [{"id": "id", "score": 1.0, "values": [1.0], "metadata": {}}]
        }
        query_resp = await client.query(
            vector=[1.0],
            top_k=20,
            include_metadata=True,
            include_values=True,
            filter={"genres": {"$in": ["genre"]}},
        )
        assert isinstance(query_resp, QueryResponse)
        assert len(query_resp.matches) == 1

        http_session.post.assert_called_once()

    async def test_list_page(self, client: DataPlane, http_session, http_response):
        http_response.json.return_value = {
            "vectors": [{"id": "id"}],
            "pagination": {"next": "next"},
        }
        list_resp = await client.list_page(prefix="foo", limit=10, pagination_token="token")
        assert len(list_resp.vectors) == 1
        assert list_resp.pagination.next == "next"

        http_session.get.assert_called_once()

    async def test_list_all(self, client: DataPlane, http_session, http_response):
        http_response.json.side_effect = [
            {
                "vectors": [{"id": "id"}],
                "pagination": {"next": "next"},
            },
            {
                "vectors": [],
            },
        ]

        async for vector_id in client.list_all(prefix="foo", page_size=1):
            assert vector_id == "id"

        assert http_session.get.call_count == 2

    async def test_delete_all(self, client: DataPlane, http_session):
        await client.delete_all(timeout=1)

        http_session.post.assert_called_once()

    async def test_delete_by_id_prefix(self, client: DataPlane, http_session, http_response):
        http_response.json.side_effect = [
            {
                "vectors": [{"id": "id1"}],
                "pagination": {"next": "next"},
            },
            {
                "vectors": [{"id": "id2"}],
                "pagination": {"next": "next"},
            },
            {
                "vectors": [],
            },
        ]

        await client.delete_by_id_prefix(id_prefix="prefix", batch_size=1, batch_timeout=1)

        assert http_session.post.call_count == 2
        assert http_session.get.call_count == 3

    def test_batchify(self, client: DataPlane):
        vectors = [Vector(id=str(i), values=[i]) for i in range(10)]
        batches = list(client._batchify(vectors, size=2))
        assert len(batches) == 5
        assert len(batches[0]) == 2
        assert len(batches[1]) == 2
        assert len(batches[2]) == 2
        assert len(batches[3]) == 2
        assert len(batches[4]) == 2

    async def test_async_batchify(self, client: DataPlane):
        async def async_iter():
            await asyncio.sleep(0)
            for i in range(10):
                yield Vector(id=str(i), values=[i])

        async for batch in client._async_batchify(async_iter(), size=2):
            assert len(batch) == 2

    def test_estimate_upsert_batch_size(self, client: DataPlane):
        vector_dimension = int(2 * 1024 * 1024 / 4)
        vector = Vector(id="id", values=[1.0] * vector_dimension, metadata={})
        batch_size = client._estimate_upsert_batch_size([vector])
        assert batch_size == 1
        client._upsert_batch_size = None

        vector_dimension = 1024
        vector = Vector(id="id", values=[1.0] * vector_dimension, metadata={})
        batch_size = client._estimate_upsert_batch_size([vector])
        assert batch_size == 511
        client._upsert_batch_size = None
