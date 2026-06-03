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
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import AsyncGenerator
from unittest.mock import patch

import pytest
from httpx import AsyncClient
from nats.aio.msg import Msg

from nucliadb.common.maindb.driver import Driver
from nucliadb.export_import.utils import get_processor_bm, get_writer_bm
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb.search.app import application
from nucliadb.standalone.settings import Settings
from nucliadb.writer import API_PREFIX
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_protos import writer_pb2
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
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
from tests.ndbfixtures.ingest import broker_resource
from tests.ndbfixtures.nidx import SEARCHER_REFRESH_INTERVAL_SECONDS
from tests.ndbfixtures.utils import create_api_client_factory
from tests.utils.dirty_index import wait_for_sync

# Main fixtures


@pytest.fixture(scope="function")
async def cluster_nucliadb_search(
    nidx,
    storage: Storage,
    nats_server: str,
    maindb_driver: Driver,
    transaction_utility: TransactionUtility,
) -> AsyncIterator[AsyncClient]:
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
        async with application.router.lifespan_context(application):
            client_factory = create_api_client_factory(application)
            async with client_factory(roles=[NucliaDBRoles.READER]) as client:
                yield client

        # TODO: fix this awful global state manipulation
        clear_global_cache()


@pytest.fixture(scope="function")
async def standalone_nucliadb_search(standalone_nucliadb: Settings) -> AsyncIterator[AsyncClient]:
    async with AsyncClient(
        base_url=f"http://localhost:{standalone_nucliadb.http_port}/{API_PREFIX}/v1",
        headers={
            "X-NUCLIADB-ROLES": "READER",
            "X-NUCLIADB-USER": "ndbtests",
        },
        timeout=None,
        event_hooks={"request": [wait_for_sync]},
    ) as client:
        yield client


@asynccontextmanager
async def nidx_sync_indexing(
    pubsub: PubSubDriver, expect: int, *, timeout: float = 10.0
) -> AsyncGenerator[None]:
    """Provide the illusion of sync indexing in nidx. This is useful in the
    distributed/cluster tests, where we want to wait for N resources being
    indexed.

    This is faster and more efficient than polling nidx for counts or other
    methods that have been implemented around in our test suite for a while

    """
    indexed = 0
    done = asyncio.Event()

    async def handle(msg: Msg) -> None:
        nonlocal pubsub, indexed, expect

        data = pubsub.parse(msg)
        notification = writer_pb2.Notification()
        notification.ParseFromString(data)

        if notification.action == writer_pb2.Notification.Action.INDEXED:
            indexed += 1
            if indexed >= expect:
                done.set()

    subscription_id = str(uuid.uuid4())
    await pubsub.subscribe(
        handler=handle,
        key=const.PubSubChannels.RESOURCE_NOTIFY.format(kbid="*"),
        group="tests",
        subscription_id=subscription_id,
    )

    try:
        yield

        # After exiting the context manager, we must wait for the expected
        # resources to be indexed and raise an error if nidx didn't do it on
        # time
        try:
            await asyncio.wait_for(done.wait(), timeout)
        except TimeoutError:
            raise Exception("Fixture setup error: nidx didn't index the expected resources on time")

        # Once everything is indexed, we wait for the searcher to refresh it's
        # contents to make sure data is searchable
        await asyncio.sleep(SEARCHER_REFRESH_INTERVAL_SECONDS + 0.1)

    finally:
        # make sure to cleanup pubsub even if the task is cancelled
        await pubsub.unsubscribe(subscription_id)


# Rest, TODO keep cleaning


@pytest.fixture(scope="function")
async def test_search_resource(
    processor: Processor,
    knowledgebox: str,
    pubsub: PubSubDriver,
):
    """
    Create a resource that has every possible bit of information
    """
    async with nidx_sync_indexing(pubsub, expect=2, timeout=20.0):
        message = broker_resource(
            knowledgebox, rid="68b6e3b747864293b71925b7bacaee7c", slug="foobar-slug"
        )
        message_writer = get_writer_bm(message)
        message_processor = get_processor_bm(message)

        await processor.process(message=message_writer, seqid=1)
        await processor.process(message=message_processor, seqid=2)

    yield knowledgebox


@pytest.fixture(scope="function")
async def multiple_search_resource(
    processor: Processor,
    knowledgebox: str,
    pubsub: PubSubDriver,
):
    """
    Create 25 resources that have every possible bit of information
    """

    n_resources = 25
    async with nidx_sync_indexing(pubsub, expect=n_resources * 2, timeout=20.0):
        for seqid in range(1, n_resources * 2 + 1, 2):
            message = broker_resource(knowledgebox)
            message_writer = get_writer_bm(message)
            message_processor = get_processor_bm(message)

            await processor.process(message=message_writer, seqid=seqid)
            await processor.process(message=message_processor, seqid=seqid + 1)

    yield knowledgebox
