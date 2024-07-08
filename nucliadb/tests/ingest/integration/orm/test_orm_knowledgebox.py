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
from unittest.mock import AsyncMock

import pytest

from nucliadb.common import datamanagers
from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox, chunker
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxConfig, SemanticModelMetadata
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility
from tests.ingest.fixtures import broker_resource


@pytest.fixture(scope="function")
async def shard_manager(
    storage: Storage,
    maindb_driver: Driver,
):
    manager = AsyncMock()
    original = get_utility(Utility.SHARD_MANAGER)
    set_utility(Utility.SHARD_MANAGER, manager)

    yield manager

    if original is None:
        clean_utility(Utility.SHARD_MANAGER)
    else:
        set_utility(Utility.SHARD_MANAGER, original)


@pytest.mark.asyncio
async def test_create_knowledgebox(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: cluster_manager.KBShardManager,
):
    count = await count_all_kbs(maindb_driver)
    assert count == 0

    model = SemanticModelMetadata(
        similarity_function=VectorSimilarity.COSINE,
        vector_dimension=384,
    )
    async with maindb_driver.transaction() as txn:
        kbid = await KnowledgeBox.create(
            txn,
            slug="test",
            config=KnowledgeBoxConfig(title="My Title 1"),
            semantic_model=model,
        )
        assert kbid
        await txn.commit()

    with pytest.raises(KnowledgeBoxConflict):
        async with maindb_driver.transaction() as txn:
            await KnowledgeBox.create(
                txn,
                slug="test",
                config=KnowledgeBoxConfig(title="My Title 2"),
                semantic_model=model,
            )

    async with maindb_driver.transaction() as txn:
        kbid2 = await KnowledgeBox.create(
            txn,
            slug="test2",
            config=KnowledgeBoxConfig(title="My Title 3"),
            semantic_model=model,
        )
        assert kbid2
        await txn.commit()

    count = await count_all_kbs(maindb_driver)
    assert count == 2

    async with maindb_driver.transaction() as txn:
        kbid = await KnowledgeBox.delete(txn, kbid2)
        assert kbid
        await txn.commit()

    count = await count_all_kbs(maindb_driver)
    assert count == 1


async def count_all_kbs(driver: Driver):
    count = 0
    async with driver.transaction(read_only=True) as txn:
        async for _ in datamanagers.kb.get_kbs(txn):
            count += 1
    return count


@pytest.mark.asyncio
async def test_knowledgebox_purge_handles_unexisting_shard_payload(
    storage: Storage, maindb_driver: Driver
):
    await KnowledgeBox.purge(maindb_driver, "idonotexist")


def test_chunker():
    total_items = 100
    chunk_size = 10
    iterations = 0
    for chunk in chunker(list(range(total_items)), chunk_size):
        assert len(chunk) == chunk_size
        assert chunk == list(range(iterations * chunk_size, (iterations * chunk_size) + chunk_size))
        iterations += 1

    assert iterations == total_items / chunk_size

    iterations = 0
    for chunk in chunker([], 2):
        iterations += 1
    assert iterations == 0


@pytest.mark.asyncio
async def test_knowledgebox_delete_all_kb_keys(
    storage,
    cache,
    fake_node,
    maindb_driver,
    knowledgebox_ingest: str,
):
    async with maindb_driver.transaction() as txn:
        kbid = knowledgebox_ingest
        kb_obj = KnowledgeBox(txn, storage, kbid=kbid)

        # Create some resources in the KB
        n_resources = 100
        uuids = set()
        for _ in range(n_resources):
            bm = broker_resource(kbid)
            r = await kb_obj.add_resource(uuid=bm.uuid, slug=bm.uuid, basic=bm.basic)
            assert r is not None
            await r.set_slug()
            uuids.add(bm.uuid)
        await txn.commit()

    # Check that all of them are there
    async with maindb_driver.transaction() as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=kbid)
        for uuid in uuids:
            assert await kb_obj.get_resource_uuid_by_slug(uuid) == uuid
        await txn.abort()

    # Now delete all kb keys
    await KnowledgeBox.delete_all_kb_keys(maindb_driver, kbid, chunk_size=10)

    # Check that all of them were deleted
    async with maindb_driver.transaction() as txn:
        kb_obj = KnowledgeBox(txn, storage, kbid=kbid)
        for uuid in uuids:
            assert await kb_obj.get_resource_uuid_by_slug(uuid) is None
        await txn.abort()
