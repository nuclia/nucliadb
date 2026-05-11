# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import asyncio

import aiohttp
import prometheus_client
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from nucliadb_telemetry.aiohttp import InstrumentedClientSession


def sample(name: str, labels: dict) -> float:
    """Read a single Prometheus sample value, defaulting to 0.0 if not yet observed."""
    return prometheus_client.REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.fixture
async def slow_server():
    """A minimal aiohttp server. Pass ?delay=<seconds> to simulate a slow response."""

    async def handler(request: web.Request) -> web.Response:
        delay = float(request.query.get("delay", 0))
        if delay:
            await asyncio.sleep(delay)
        return web.Response(text="ok")

    app = web.Application()
    app.router.add_get("/", handler)
    server = TestServer(app)
    await server.start_server()
    yield server
    await server.close()


async def test_new_connection_is_counted(slow_server):
    pool = "test_new_conn"
    before = sample("aiohttp_pool_connections_total", {"pool": pool, "type": "new"})

    async with InstrumentedClientSession(pool) as session:
        async with session.get(slow_server.make_url("/")) as resp:
            await resp.read()

    after = sample("aiohttp_pool_connections_total", {"pool": pool, "type": "new"})
    assert after - before == 1


async def test_reused_connection_is_counted(slow_server):
    pool = "test_reused_conn"
    before_reused = sample("aiohttp_pool_connections_total", {"pool": pool, "type": "reused"})

    async with InstrumentedClientSession(pool) as session:
        # First request opens a new connection; second reuses it.
        for _ in range(2):
            async with session.get(slow_server.make_url("/")) as resp:
                await resp.read()

    after_reused = sample("aiohttp_pool_connections_total", {"pool": pool, "type": "reused"})
    assert after_reused - before_reused == 1


async def test_inflight_returns_to_zero_after_request(slow_server):
    pool = "test_inflight"

    async with InstrumentedClientSession(pool) as session:
        async with session.get(slow_server.make_url("/")) as resp:
            await resp.read()

    assert sample("aiohttp_pool_inflight_requests", {"pool": pool}) == 0


async def test_pool_wait_recorded_when_pool_is_exhausted(slow_server):
    """With limit=1, a second concurrent request must queue behind the first.
    The wait histogram should record at least one observation."""
    pool = "test_queuing"
    before = sample("aiohttp_pool_wait_seconds_count", {"pool": pool})

    connector = aiohttp.TCPConnector(limit=1)
    async with InstrumentedClientSession(pool, connector=connector) as session:
        url = slow_server.make_url("/?delay=0.05")
        # Both requests start concurrently; the second must wait for the first
        # to release the single permitted connection.
        await asyncio.gather(
            session.get(url).__aenter__(),
            session.get(url).__aenter__(),
        )

    after = sample("aiohttp_pool_wait_seconds_count", {"pool": pool})
    assert after - before >= 1
