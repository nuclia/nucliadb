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
import time
import uuid
from typing import AsyncIterator

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.orm.resource import Resource
from nucliadb.tests.vectors import V1
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX
from nucliadb_protos import utils_pb2 as upb
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_utils.storages.storage import Storage
from tests.utils.broker_messages import BrokerMessageBuilder


@pytest.fixture(scope="function")
async def knowledgebox(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: KBShardManager,
):
    kbid = KnowledgeBox.new_unique_kbid()
    kbslug = "slug-" + str(uuid.uuid4())
    model = SemanticModelMetadata(
        similarity_function=upb.VectorSimilarity.COSINE, vector_dimension=len(V1)
    )
    await KnowledgeBox.create(
        maindb_driver, kbid=kbid, slug=kbslug, semantic_models={"my-semantic-model": model}
    )

    yield kbid

    await KnowledgeBox.delete(maindb_driver, kbid)
    # await KnowledgeBox.purge(maindb_driver, kbid)


# FIXME: this is a weird situation, we can use a hosted-like nucliadb while this
# creates a KB as it was onprem. The end result should not change much but still, is
# something we may want to fix
@pytest.fixture(scope="function")
async def knowledgebox_by_api(nucliadb_writer_manager: AsyncClient):
    kbslug = "slug-" + str(uuid.uuid4())
    resp = await nucliadb_writer_manager.post(
        f"/{KBS_PREFIX}",
        json={
            "slug": kbslug,
            "title": "My Test Knowledge Box",
        },
    )
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")
    assert kbid is not None

    yield kbid

    resp = await nucliadb_writer_manager.delete(
        f"/{KB_PREFIX}/{kbid}",
    )
    assert resp.status_code == 200


# Used by: nucliadb standalone tests
@pytest.fixture(scope="function")
async def knowledgebox_one(knowledgebox_by_api: str):
    yield knowledgebox_by_api


# Used by: nucliadb writer tests
@pytest.fixture(scope="function")
async def knowledgebox_writer(knowledgebox_by_api: str):
    yield knowledgebox_by_api


@pytest.fixture(scope="function")
async def full_resource(
    storage: Storage,
    maindb_driver: Driver,
    dummy_index_node_cluster,
    knowledgebox: str,
) -> AsyncIterator[Resource]:
    """Create a resource in `knowledgebox` that has every possible bit of information.

    DISCLAIMER: this comes from a really old fixture, so probably it does
    **not** contains every possible bit of information.

    """
    from tests.ingest.fixtures import create_resource

    resource = await create_resource(
        storage=storage,
        driver=maindb_driver,
        knowledgebox_ingest=knowledgebox,
    )
    yield resource
    resource.clean()


# Used by: nucliadb writer tests
@pytest.fixture(scope="function")
async def resource(nucliadb_writer: AsyncClient, knowledgebox_writer: str):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox_writer}/resources",
        json={
            "slug": "resource1",
            "title": "Resource 1",
        },
    )
    assert resp.status_code == 201
    uuid = resp.json()["uuid"]

    return uuid


@pytest.fixture(scope="function")
async def simple_resources(
    maindb_driver: Driver, processor: Processor, knowledgebox: str
) -> AsyncIterator[tuple[str, list[str]]]:
    """Create a set of resources with basic information on `knowledgebox`."""
    total = 10
    resource_ids = []

    for i in range(1, total + 1):
        slug = f"simple-resource-{i}"
        bmb = BrokerMessageBuilder(kbid=knowledgebox, slug=slug)
        bmb.with_title(f"My simple resource {i}")
        bmb.with_summary(f"Summary of my simple resource {i}")
        bm = bmb.build()
        await processor.process(message=bm, seqid=i)
        resource_ids.append(bm.uuid)

    # Give processed data some time to be processed
    timeout = 5
    start = time.time()
    created_count = 0
    while created_count < total or (time.time() - start) < timeout:
        created_count = len(
            [rid async for rid in datamanagers.resources.iterate_resource_ids(kbid=knowledgebox)]
        )
        await asyncio.sleep(0.1)

    yield knowledgebox, resource_ids
