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

import httpx
import pytest

from nucliadb_utils.aiopynecone.client import (
    MAX_DELETE_BATCH_SIZE,
    ControlPlane,
    DataPlane,
    PineconeAPIError,
    PineconeRateLimitError,
    PineconeSession,
    async_batchify,
    batchify,
    raise_for_status,
)
from nucliadb_utils.aiopynecone.exceptions import (
    PineconeNeedsPlanUpgradeError,
    RetriablePineconeAPIError,
)
from nucliadb_utils.aiopynecone.models import CreateIndexRequest, QueryResponse, Vector


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
        session_mock.get = unittest.mock.AsyncMock(return_value=http_response)
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

    async def test_describe_index(self, client: ControlPlane, http_session, http_response):
        http_response.json.return_value = {
            "dimension": 10,
            "metric": "dot",
            "host": "host",
            "name": "index-name",
            "spec": {},
            "status": {
                "ready": True,
                "state": "Initialized",
            },
        }
        index = await client.describe_index(name="index-name")
        assert index.dimension == 10
        assert index.host == "host"
        assert index.metric == "dot"
        assert index.name == "index-name"
        assert index.spec == {}
        assert index.status.ready is True


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
        await client.upsert(vectors=[Vector(id="id", values=[1.0])], timeout=1)

        http_session.post.assert_called_once()

    async def test_upsert_in_batches(self, client: DataPlane, http_session):
        await client.upsert_in_batches(vectors=[Vector(id="id", values=[1.0])], max_parallel_batches=1)

        http_session.post.assert_called_once()

    async def test_delete(self, client: DataPlane, http_session):
        await client.delete(ids=["id"])

        http_session.post.assert_called_once()

        with pytest.raises(ValueError):
            await client.delete(["id"] * (MAX_DELETE_BATCH_SIZE + 1))

    async def test_stats(self, client: DataPlane, http_session, http_response):
        http_response.json.return_value = {
            "dimension": 10,
            "namespaces": {
                "": {
                    "vectorCount": 10,
                },
                "namespace": {
                    "vectorCount": 10,
                },
            },
            "totalVectorCount": 20,
        }
        stats = await client.stats()
        assert stats.dimension == 10
        assert stats.namespaces[""].vectorCount == 10
        assert stats.namespaces["namespace"].vectorCount == 10
        assert stats.totalVectorCount == 20

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
            timeout=1,
        )
        assert isinstance(query_resp, QueryResponse)
        assert len(query_resp.matches) == 1

        http_session.post.assert_called_once()

    async def test_list_page(self, client: DataPlane, http_session, http_response):
        http_response.json.return_value = {
            "vectors": [{"id": "id"}],
            "pagination": {"next": "next"},
        }
        list_resp = await client.list_page(id_prefix="foo", limit=10, pagination_token="token")
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

        async for vector_id in client.list_all(id_prefix="foo", page_size=1):
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

    async def test_delete_by_id_prefix_large_batch_size(
        self, client: DataPlane, http_session, http_response
    ):
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
        await client.delete_by_id_prefix(id_prefix="prefix", batch_size=40_000, batch_timeout=1)

    def test_estimate_upsert_batch_size(self, client: DataPlane):
        vector_dimension = int(2 * 1024 * 1024 / 4)
        vector = Vector(id="id", values=[1.0] * vector_dimension, metadata={})
        batch_size = client._estimate_upsert_batch_size([vector])
        assert batch_size == 1
        # Since the value is cached, the batch size should be the same
        assert client._estimate_upsert_batch_size([]) == batch_size
        client._upsert_batch_size = None

        vector_dimension = 1024
        vector = Vector(id="id", values=[1.0] * vector_dimension, metadata={})
        batch_size = client._estimate_upsert_batch_size([vector])
        assert batch_size == 511
        client._upsert_batch_size = None

    async def test_client_retries_on_rate_limit_errors(
        self, client: DataPlane, http_session, http_response
    ):
        http_response.status_code = 429
        http_response.json.return_value = {"error": {"code": 4, "message": "rate limit error"}}
        http_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "rate limit error", request=None, response=http_response
        )
        with pytest.raises(PineconeRateLimitError):
            await client.query(vector=[1.0])
        assert http_session.post.call_count == 4

    @pytest.fixture(scope="function")
    def metrics_registry(self):
        import prometheus_client.registry

        # Clear all metrics before each test
        for collector in prometheus_client.registry.REGISTRY._names_to_collectors.values():
            if not hasattr(collector, "_metrics"):
                continue
            collector._metrics.clear()
        yield prometheus_client.registry.REGISTRY

    async def test_client_records_metrics(self, client: DataPlane, metrics_registry, http_response):
        http_response.json.return_value = {"matches": []}
        await client.query(vector=[1.0])

        assert (
            metrics_registry.get_sample_value(
                "pinecone_client_duration_seconds_count", {"type": "query"}
            )
            > 0
        )


def test_batchify():
    iterable = list(range(10))
    batches = list(batchify(iterable, batch_size=2))
    assert len(batches) == 5
    for batch in batches:
        assert len(batch) == 2


async def test_async_batchify():
    async def async_iter():
        await asyncio.sleep(0)
        for i in range(10):
            yield i

    async_iterable = async_iter()

    batches = 0
    async for batch in async_batchify(async_iterable, batch_size=2):
        batches += 1
        assert len(batch) == 2
    assert batches == 5


def test_raise_for_status_5xx():
    request = unittest.mock.MagicMock()
    response = unittest.mock.MagicMock(status_code=500)
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "message", request=request, response=response
    )
    with pytest.raises(RetriablePineconeAPIError):
        raise_for_status("op", response)


def test_raise_for_status_4xx():
    request = unittest.mock.MagicMock()
    response = unittest.mock.MagicMock(status_code=412)
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "message", request=request, response=response
    )
    with pytest.raises(PineconeAPIError):
        raise_for_status("op", response)


def test_raise_for_status_rate_limit():
    request = unittest.mock.MagicMock()
    response = unittest.mock.MagicMock(status_code=429)
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "message", request=request, response=response
    )
    with pytest.raises(PineconeRateLimitError):
        raise_for_status("op", response)


def test_raise_for_status_monthly_rate_limit():
    request = unittest.mock.MagicMock()
    response = unittest.mock.MagicMock(
        status_code=429,
        json=unittest.mock.Mock(
            return_value={
                "message": "Request failed. You've reached your write unit limit for the current month (2000000). To continue writing data, upgrade your plan.",  # noqa
                "code": 8,
                "details": [],
            }
        ),
    )
    response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Request failed. You've reached your write unit limit for the current month (2000000). To continue writing data, upgrade your plan.",  # noqa
        request=request,
        response=response,
    )
    with pytest.raises(PineconeNeedsPlanUpgradeError):
        raise_for_status("op", response)


def test_index_name_validation():
    # Only lowercase letters, numbers and dashes are allowed
    with pytest.raises(ValueError):
        CreateIndexRequest(
            name="foo_bar",
            dimension=1,
            metric="dot",
        )
    # 45 characters is the maximum length
    with pytest.raises(ValueError):
        CreateIndexRequest(
            name="f" * 46,
            dimension=1,
            metric="dot",
        )
    CreateIndexRequest(
        name="kbid--default",
        dimension=1,
        metric="dot",
    )


def test_vector_metadata_validation():
    vec = Vector(id="foo", values=[1.0], metadata={"key": "value"})
    assert vec.metadata == {"key": "value"}
    with pytest.raises(ValueError):
        # If metadata exceeds 40 kB, it should raise an error
        big_metadata = {key: "value" * 1000 for key in range(50)}
        Vector(id="foo", values=[1.0], metadata=big_metadata)
