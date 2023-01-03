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


import asyncio
from enum import Enum
from os.path import dirname
from typing import Dict, List, Optional

import pytest
from httpx import AsyncClient
from nucliadb_protos.nodereader_pb2 import GetShardRequest
from nucliadb_protos.noderesources_pb2 import Shard
from redis import asyncio as aioredis
from starlette.routing import Mount

from nucliadb.ingest.cache import clear_ingest_cache
from nucliadb.ingest.orm import NODES
from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.tests.fixtures import broker_resource
from nucliadb.ingest.utils import get_driver
from nucliadb.search import API_PREFIX
from nucliadb_utils.utilities import clear_global_cache


def free_port() -> int:
    import socket

    sock = socket.socket()
    sock.bind(("", 0))
    return sock.getsockname()[1]


@pytest.fixture(scope="function")
def test_settings_search(gcs, redis, node):  # type: ignore
    from nucliadb.ingest.settings import settings as ingest_settings
    from nucliadb_utils.cache.settings import settings as cache_settings
    from nucliadb_utils.settings import (
        nuclia_settings,
        nucliadb_settings,
        running_settings,
        storage_settings,
    )
    from nucliadb_utils.storages.settings import settings as extended_storage_settings

    storage_settings.gcs_endpoint_url = gcs
    storage_settings.file_backend = "gcs"
    storage_settings.gcs_bucket = "test_{kbid}"

    extended_storage_settings.gcs_indexing_bucket = "indexing"
    extended_storage_settings.gcs_deadletter_bucket = "deadletter"

    url = f"redis://{redis[0]}:{redis[1]}"
    cache_settings.cache_pubsub_driver = "redis"
    cache_settings.cache_pubsub_channel = "pubsub-nuclia"
    cache_settings.cache_pubsub_redis_url = url

    running_settings.debug = False

    ingest_settings.pull_time = 0
    ingest_settings.driver = "redis"
    ingest_settings.driver_redis_url = url
    ingest_settings.nuclia_partitions = 1

    nuclia_settings.dummy_processing = True
    ingest_settings.grpc_port = free_port()

    nucliadb_settings.nucliadb_ingest = f"localhost:{ingest_settings.grpc_port}"

    extended_storage_settings.local_testing_files = f"{dirname(__file__)}"


@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def search_api(
    redis,
    transaction_utility,
    indexing_utility_registered,
    test_settings_search: None,
    event_loop,
):  # type: ignore
    from nucliadb.ingest.orm import NODES
    from nucliadb.search.app import application

    async def handler(req, exc):  # type: ignore
        raise exc

    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()

    # Little hack to raise exeptions from VersionedFastApi
    for route in application.routes:
        if isinstance(route, Mount):
            route.app.middleware_stack.handler = handler  # type: ignore

    await application.router.startup()

    # Make sure is clean
    await asyncio.sleep(1)
    count = 0
    while len(NODES) < 2:
        print("awaiting cluster nodes - search fixtures.py")
        await asyncio.sleep(4)
        if count == 10:
            raise Exception("No cluster")
        count += 1

    def make_client_fixture(
        roles: Optional[List[Enum]] = None,
        user: str = "",
        version: str = "1",
        root: bool = False,
        extra_headers: Optional[Dict[str, str]] = None,
    ) -> AsyncClient:
        roles = roles or []
        client_base_url = "http://test"

        if root is False:
            client_base_url = f"{client_base_url}/{API_PREFIX}/v{version}"

        client = AsyncClient(app=application, base_url=client_base_url)  # type: ignore
        client.headers["X-NUCLIADB-ROLES"] = ";".join([role.value for role in roles])
        client.headers["X-NUCLIADB-USER"] = user

        extra_headers = extra_headers or {}
        if len(extra_headers) == 0:
            return client

        for header, value in extra_headers.items():
            client.headers[f"{header}"] = value

        return client

    yield make_client_fixture
    await application.router.shutdown()
    # Make sure nodes can sync
    await asyncio.sleep(1)
    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()
    clear_ingest_cache()
    clear_global_cache()
    for node in NODES.values():
        node._reader = None
        node._writer = None
        node._sidecar = None


@pytest.fixture(scope="function")
async def test_search_resource(
    indexing_utility_registered,
    processor,
    knowledgebox_ingest,
):
    """
    Create a resource that has every possible bit of information
    """
    message1 = broker_resource(knowledgebox_ingest, rid="foobar", slug="foobar-slug")

    return await inject_message(processor, knowledgebox_ingest, message1)


@pytest.fixture(scope="function")
async def test_resource_deterministic_ids(
    indexing_utility_registered,
    processor,
    knowledgebox_ingest,
):
    rid = "foobar"
    slug = "foobar-slug"
    message1 = broker_resource(knowledgebox_ingest, rid=rid, slug=slug)
    kb = await inject_message(processor, knowledgebox_ingest, message1)
    return kb, rid, slug


@pytest.fixture(scope="function")
async def multiple_search_resource(
    indexing_utility_registered,
    processor,
    knowledgebox_ingest,
):
    """
    Create a resource that has every possible bit of information
    """
    for count in range(100):
        message1 = broker_resource(knowledgebox_ingest)
        await inject_message(processor, knowledgebox_ingest, message1, count + 1)
    return knowledgebox_ingest


async def inject_message(processor, knowledgebox_ingest, message, count: int = 1):
    await processor.process(message=message, seqid=count)

    # Make sure is indexed
    driver = await get_driver()
    txn = await driver.begin()
    shard = await Node.actual_shard(txn, knowledgebox_ingest)
    if shard is None:
        raise Exception("Could not find shard")
    await txn.abort()

    checks: Dict[str, bool] = {}
    for replica in shard.shard.replicas:
        if replica.shard.id not in checks:
            checks[replica.shard.id] = False

    for i in range(30):
        for replica in shard.shard.replicas:
            node_obj = NODES.get(replica.node)
            if node_obj is not None:
                req = GetShardRequest()
                req.shard_id.id = replica.shard.id
                count_shard: Shard = await node_obj.reader.GetShard(req)  # type: ignore
                if count_shard.resources >= count:
                    checks[replica.shard.id] = True
                else:
                    checks[replica.shard.id] = False

        if all(checks.values()):
            break
        await asyncio.sleep(1)

    assert all(checks.values())
    return knowledgebox_ingest
