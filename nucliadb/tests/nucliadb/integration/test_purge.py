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
import random
import uuid
from typing import cast
from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.cluster import manager
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb.ingest.orm.knowledgebox import (
    KB_TO_DELETE_BASE,
    KB_TO_DELETE_STORAGE_BASE,
)
from nucliadb.purge import purge_kb, purge_kb_storage
from nucliadb.purge.orphan_shards import detect_orphan_shards, purge_orphan_shards
from nucliadb_protos import nodewriter_pb2, utils_pb2, writer_pb2
from nucliadb_utils.storages.storage import Storage


@pytest.mark.asyncio
async def test_purge_deletes_everything_from_maindb(
    maindb_driver: Driver,
    storage: Storage,
    nucliadb_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    """Create a KB and some resource and then purge it. Validate that purge
    removes every key from maindb

    """
    kb_slug = str(uuid.uuid4())
    resp = await nucliadb_manager.post("/kbs", json={"slug": kb_slug})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    resp = await nucliadb_manager.get("/kbs")
    body = resp.json()
    assert len(body["kbs"]) == 1
    assert body["kbs"][0]["uuid"] == kbid

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
        },
    )
    assert resp.status_code == 201

    # Maindb now contain keys for the new kb and resource
    keys_after_create = await list_all_keys(maindb_driver)
    assert len(keys_after_create) > 0

    assert await kb_catalog_entries_count(maindb_driver, kbid) > 0

    resp = await nucliadb_manager.delete(f"/kb/{kbid}")
    assert resp.status_code == 200

    resp = await nucliadb_manager.get("/kbs")
    body = resp.json()
    assert len(body["kbs"]) == 0

    keys_after_delete = await list_all_keys(maindb_driver)
    # A marker key has been added to delete the KB asynchronously
    assert any([key.startswith(KB_TO_DELETE_BASE) for key in keys_after_delete])

    await purge_kb(maindb_driver)
    keys_after_purge_kb = await list_all_keys(maindb_driver)
    # A marker key has been added to delete storage when bucket is empty (that
    # can take a while so it will happen asynchronously too)
    assert any([key.startswith(KB_TO_DELETE_STORAGE_BASE) for key in keys_after_purge_kb])

    # Catalog entries should be deleted too at this point
    assert await kb_catalog_entries_count(maindb_driver, kbid) == 0

    await purge_kb_storage(maindb_driver, storage)

    # After deletion and purge, no keys should be in maindb
    keys_after_purge_storage = await list_all_keys(maindb_driver)
    assert len(keys_after_purge_storage) == 0


@pytest.mark.asyncio
async def test_purge_orphan_shards(
    maindb_driver: Driver,
    storage: Storage,
    nucliadb_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    """Create a KB with some resource (hence a shard) and delete it. Simulate an
    index node is down and validate orphan shards purge works as expected.

    """
    kb_slug = str(uuid.uuid4())
    resp = await nucliadb_manager.post("/kbs", json={"slug": kb_slug})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My title",
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
        },
    )
    assert resp.status_code == 201

    resp = await nucliadb_manager.delete(f"/kb/{kbid}")
    assert resp.status_code == 200

    # Purge while index nodes are not available
    index_nodes = manager.INDEX_NODES
    manager.INDEX_NODES = type(manager.INDEX_NODES)()

    await purge_kb(maindb_driver)

    manager.INDEX_NODES = index_nodes

    # We have removed the shards in maindb but left them orphan in the index
    # nodes
    async with maindb_driver.transaction() as txn:
        maindb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        assert maindb_shards is None

    shards = []
    for node in manager.get_index_nodes():
        shards.extend(await node.list_shards())
    assert len(shards) > 0

    orphan_shards = await detect_orphan_shards(maindb_driver)
    assert len(orphan_shards) == len(shards)

    # Purge orphans and validate
    await purge_orphan_shards(maindb_driver)

    shards = []
    for node in manager.get_index_nodes():
        shards.extend(await node.list_shards())
    assert len(shards) == 0


@pytest.mark.asyncio
async def test_purge_orphan_shard_detection(
    maindb_driver: Driver,
    storage: Storage,
    nucliadb_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    """Prepare a situation where there are:
    - a regular KB
    - an orphan shard
    - a shard from a rollover

    Then, validate orphan shard detection find only the true orphan shard.
    """
    # Regular KB
    kb_slug = str(uuid.uuid4())
    resp = await nucliadb_manager.post("/kbs", json={"slug": kb_slug})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    # Orphan shard
    available_nodes = manager.get_index_nodes()
    node = random.choice(available_nodes)
    orphan_shard = await node.new_shard(
        kbid="deleted-kb",
        vector_index_config=nodewriter_pb2.VectorIndexConfig(
            similarity=utils_pb2.VectorSimilarity.COSINE,
            normalize_vectors=False,
            vector_type=nodewriter_pb2.VectorType.DENSE_F32,
            vector_dimension=128,
        ),
    )
    orphan_shard_id = orphan_shard.id

    # Rollover shard
    async with maindb_driver.transaction() as txn:
        rollover_shards = writer_pb2.Shards(
            shards=[writer_pb2.ShardObject(shard="rollover-shard")],
            kbid=kbid,
        )
        await datamanagers.rollover.update_kb_rollover_shards(txn, kbid=kbid, kb_shards=rollover_shards)

    orphan_shards = await detect_orphan_shards(maindb_driver)
    assert len(orphan_shards) == 1
    assert orphan_shard_id in orphan_shards


@pytest.fixture(scope="function")
def storage():
    storage = AsyncMock()
    storage.delete_kb = AsyncMock(return_value=(True, False))
    yield storage


async def list_all_keys(driver: Driver) -> list[str]:
    async with driver.transaction() as txn:
        keys = [key async for key in txn.keys(match="")]
    return keys


async def kb_catalog_entries_count(driver: Driver, kbid: str) -> int:
    async with driver.transaction() as txn:
        txn = cast(PGTransaction, txn)
        async with txn.connection.cursor() as cur:
            await cur.execute(
                "SELECT COUNT(*) FROM catalog WHERE kbid = %s",
                (kbid,),
            )
            count = await cur.fetchone()
            if count is None:
                return 0
            return count[0]
