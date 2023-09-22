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

from nucliadb.common.cluster.manager import KBShardManager, get_index_node
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.cache import clear_ingest_cache
from nucliadb.ingest.tests.fixtures import broker_resource
from nucliadb.search import API_PREFIX
from nucliadb_utils.tests import free_port
from nucliadb_utils.utilities import clear_global_cache


@pytest.fixture(scope="function")
def test_settings_search(gcs, natsd, node, maindb_driver):  # type: ignore
    from nucliadb.ingest.settings import settings as ingest_settings
    from nucliadb_utils.cache.settings import settings as cache_settings
    from nucliadb_utils.settings import (
        FileBackendConfig,
        nuclia_settings,
        nucliadb_settings,
        running_settings,
        storage_settings,
    )
    from nucliadb_utils.storages.settings import settings as extended_storage_settings

    storage_settings.gcs_endpoint_url = gcs
    storage_settings.file_backend = FileBackendConfig.GCS
    storage_settings.gcs_bucket = "test_{kbid}"

    extended_storage_settings.gcs_indexing_bucket = "indexing"
    extended_storage_settings.gcs_deadletter_bucket = "deadletter"

    cache_settings.cache_pubsub_nats_url = [natsd]

    running_settings.debug = False

    ingest_settings.disable_pull_worker = True

    ingest_settings.nuclia_partitions = 1

    nuclia_settings.dummy_processing = True
    nuclia_settings.dummy_predict = True

    ingest_settings.grpc_port = free_port()

    nucliadb_settings.nucliadb_ingest = f"localhost:{ingest_settings.grpc_port}"

    extended_storage_settings.local_testing_files = f"{dirname(__file__)}"


@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def search_api(test_settings_search, transaction_utility, redis):  # type: ignore
    from nucliadb.common.cluster import manager
    from nucliadb.search.app import application

    driver = aioredis.from_url(f"redis://{redis[0]}:{redis[1]}")
    await driver.flushall()

    await application.router.startup()

    # Make sure is clean
    await asyncio.sleep(1)
    count = 0
    while len(manager.INDEX_NODES) < 2:
        print("awaiting cluster nodes - search fixtures.py")
        await asyncio.sleep(1)
        if count == 40:
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
    await driver.flushall()
    await driver.close(close_connection_pool=True)
    clear_ingest_cache()
    clear_global_cache()
    manager.INDEX_NODES.clear()


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
    kbid = await inject_message(processor, knowledgebox_ingest, message1)
    resource_field_count = 3
    await wait_for_shard(knowledgebox_ingest, resource_field_count)
    yield kbid


@pytest.fixture(scope="function")
async def multiple_search_resource(
    indexing_utility_registered,
    processor,
    knowledgebox_ingest,
):
    """
    Create 100 resources that have every possible bit of information
    """
    n_resources = 100
    fields_per_resource = 3
    for count in range(1, n_resources + 1):
        message = broker_resource(knowledgebox_ingest)
        await processor.process(message=message, seqid=count)

    await wait_for_shard(knowledgebox_ingest, n_resources * fields_per_resource)
    return knowledgebox_ingest


async def inject_message(
    processor, knowledgebox_ingest, message, count: int = 1
) -> str:
    await processor.process(message=message, seqid=count)
    await wait_for_shard(knowledgebox_ingest, count)
    return knowledgebox_ingest


async def wait_for_shard(knowledgebox_ingest: str, count: int) -> str:
    # Make sure is indexed
    driver = get_driver()
    txn = await driver.begin()
    shard_manager = KBShardManager()
    shard = await shard_manager.get_current_active_shard(txn, knowledgebox_ingest)
    if shard is None:
        raise Exception("Could not find shard")
    await txn.abort()

    checks: Dict[str, bool] = {}
    for replica in shard.replicas:
        if replica.shard.id not in checks:
            checks[replica.shard.id] = False

    for i in range(30):
        for replica in shard.replicas:
            node_obj = get_index_node(replica.node)
            if node_obj is not None:
                req = GetShardRequest()
                req.shard_id.id = replica.shard.id
                count_shard: Shard = await node_obj.reader.GetShard(req)  # type: ignore
                if count_shard.fields >= count:
                    checks[replica.shard.id] = True
                else:
                    checks[replica.shard.id] = False

        if all(checks.values()):
            break
        await asyncio.sleep(1)

    assert all(checks.values())
    return knowledgebox_ingest
