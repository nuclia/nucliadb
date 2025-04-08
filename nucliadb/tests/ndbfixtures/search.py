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
from unittest.mock import patch

import pytest

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.nidx import get_nidx_api_client
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb.search.app import application
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_protos.nodereader_pb2 import GetShardRequest
from nucliadb_protos.noderesources_pb2 import Shard
from nucliadb_telemetry.fastapi import instrument_app
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.cache.settings import settings as cache_settings
from nucliadb_utils.settings import (
    nuclia_settings,
    nucliadb_settings,
    running_settings,
)
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.tests import free_port
from nucliadb_utils.transaction import TransactionUtility
from nucliadb_utils.utilities import (
    clear_global_cache,
)
from tests.ingest.fixtures import broker_resource
from tests.ndbfixtures import SERVICE_NAME
from tests.ndbfixtures.utils import create_api_client_factory

# Main fixtures


@pytest.fixture(scope="function")
async def cluster_nucliadb_search(
    storage: Storage,
    nats_server: str,
    nidx,
    maindb_driver: Driver,
    transaction_utility: TransactionUtility,
    nats_indexing_utility,
):
    with (
        patch.object(cache_settings, "cache_pubsub_nats_url", [nats_server]),
        patch.object(running_settings, "debug", False),
        patch.object(ingest_settings, "disable_pull_worker", True),
        patch.object(ingest_settings, "nuclia_partitions", 1),
        patch.object(nuclia_settings, "dummy_processing", True),
        patch.object(nuclia_settings, "dummy_predict", True),
        patch.object(nuclia_settings, "dummy_learning_services", True),
        patch.object(ingest_settings, "grpc_port", free_port()),
        patch.object(nucliadb_settings, "nucliadb_ingest", f"localhost:{ingest_settings.grpc_port}"),
    ):
        instrument_app(
            application,
            tracer_provider=get_telemetry(SERVICE_NAME),
            excluded_urls=["/"],
            metrics=True,
            trace_id_on_responses=True,
        )
        async with application.router.lifespan_context(application):
            client_factory = create_api_client_factory(application)
            async with client_factory(roles=[NucliaDBRoles.READER]) as client:
                yield client

        # TODO: fix this awful global state manipulation
        clear_global_cache()


# Rest, TODO keep cleaning


@pytest.fixture(scope="function")
async def test_search_resource(
    processor,
    knowledgebox_ingest,
):
    """
    Create a resource that has every possible bit of information
    """
    message1 = broker_resource(
        knowledgebox_ingest, rid="68b6e3b747864293b71925b7bacaee7c", slug="foobar-slug"
    )
    kbid = await inject_message(processor, knowledgebox_ingest, message1)
    resource_field_count = 3
    await wait_for_shard(knowledgebox_ingest, resource_field_count)
    yield kbid


@pytest.fixture(scope="function")
async def multiple_search_resource(
    processor,
    knowledgebox_ingest,
):
    """
    Create 25 resources that have every possible bit of information
    """
    n_resources = 25
    fields_per_resource = 3
    for count in range(1, n_resources + 1):
        message = broker_resource(knowledgebox_ingest)
        await processor.process(message=message, seqid=count)

    await wait_for_shard(knowledgebox_ingest, n_resources * fields_per_resource)
    return knowledgebox_ingest


async def inject_message(processor, knowledgebox_ingest, message, count: int = 1) -> str:
    await processor.process(message=message, seqid=count)
    await wait_for_shard(knowledgebox_ingest, count)
    return knowledgebox_ingest


async def wait_for_shard(knowledgebox_ingest: str, count: int) -> str:
    # Make sure is indexed
    driver = get_driver()
    async with driver.transaction() as txn:
        shard_manager = KBShardManager()
        shard = await shard_manager.get_current_active_shard(txn, knowledgebox_ingest)
        if shard is None:
            raise Exception("Could not find shard")
        await txn.abort()

    nidx_api = get_nidx_api_client()
    req = GetShardRequest()
    req.shard_id.id = shard.nidx_shard_id
    for _ in range(30):
        count_shard: Shard = await nidx_api.GetShard(req)  # type: ignore
        if count_shard.fields >= count:
            break
        await asyncio.sleep(1)
    # Wait an extra couple of seconds for reader/searcher to catch up
    await asyncio.sleep(2)
    return knowledgebox_ingest
