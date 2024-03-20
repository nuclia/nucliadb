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
import uuid
from typing import Any, Optional
from unittest.mock import MagicMock

import pytest

from nucliadb.common import datamanagers
from nucliadb.common.cluster import manager
from nucliadb.common.cluster.settings import settings
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import knowledgebox_pb2, utils_pb2, writer_pb2


def test_should_create_new_shard():
    sm = manager.KBShardManager()
    low_para_counter = {
        "num_paragraphs": settings.max_shard_paragraphs - 1,
        "num_fields": 0,
    }
    high_para_counter = {
        "num_paragraphs": settings.max_shard_paragraphs + 1,
        "num_fields": 0,
    }
    assert sm.should_create_new_shard(**low_para_counter) is False
    assert sm.should_create_new_shard(**high_para_counter) is True

    low_fields_counter = {"num_fields": settings.max_shard_fields, "num_paragraphs": 0}
    high_fields_counter = {
        "num_fields": settings.max_shard_fields + 1,
        "num_paragraphs": 0,
    }
    assert sm.should_create_new_shard(**low_fields_counter) is False
    assert sm.should_create_new_shard(**high_fields_counter) is True


@pytest.fixture(scope="function")
async def fake_node():
    manager.INDEX_NODES.clear()
    yield manager.add_index_node(
        id="node-0",
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
    )
    manager.INDEX_NODES.clear()


async def test_standalone_node_garbage_collects(fake_node):
    mng = manager.StandaloneKBShardManager()

    mng.max_ops_before_checks = 0

    await mng.add_resource(
        writer_pb2.ShardObject(
            shard="123",
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-0"
                )
            ],
        ),
        resource=MagicMock(),
        txid=-1,
        partition=0,
        kb="kb",
    )

    await asyncio.sleep(0.05)
    assert len(fake_node.writer.calls["GC"]) == 1


async def test_shard_creation(fake_index_nodes: list[str], txn: Transaction):
    """Given a cluster of index nodes, validate shard creation logic.

    Every logic shard should create a configured amount of indexing replicas and
    update the information about writable shards.

    """
    index_nodes = set(fake_index_nodes)
    kbid = f"kbid:{test_shard_creation.__name__}"
    semantic_model = knowledgebox_pb2.SemanticModelMetadata()
    release_channel = utils_pb2.ReleaseChannel.STABLE
    sm = manager.KBShardManager()

    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    assert shards is None

    # create first shard
    await sm.create_shard_by_kbid(txn, kbid, semantic_model, release_channel)

    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    assert shards is not None
    assert len(shards.shards) == 1
    assert shards.shards[0].read_only is False
    # B/c with Shards.actual
    assert shards.actual == 0
    assert set((replica.node for replica in shards.shards[0].replicas)) == index_nodes

    # adding a second shard will mark the first as read only
    await sm.create_shard_by_kbid(txn, kbid, semantic_model, release_channel)

    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    assert shards is not None
    assert len(shards.shards) == 2
    assert shards.shards[0].read_only is True
    assert shards.shards[1].read_only is False
    # B/c with Shards.actual
    assert shards.actual == 1
    assert set((replica.node for replica in shards.shards[1].replicas)) == index_nodes

    # adding a third one will be equivalent
    await sm.create_shard_by_kbid(txn, kbid, semantic_model, release_channel)

    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    assert shards is not None
    assert len(shards.shards) == 3
    assert shards.shards[0].read_only is True
    assert shards.shards[1].read_only is True
    assert shards.shards[2].read_only is False
    # B/c with Shards.actual
    assert shards.actual == 2
    assert set((replica.node for replica in shards.shards[1].replicas)) == index_nodes


@pytest.fixture
def txn():
    class MockTransaction:
        def __init__(self):
            self.store = {}

        async def get(self, key: str) -> Optional[Any]:
            return self.store.get(key, None)

        async def set(self, key: str, value: Any):
            self.store[key] = value

    yield MockTransaction()


@pytest.fixture(scope="function")
def fake_index_nodes():
    assert len(manager.INDEX_NODES) == 0, "Some test isn't cleaning global state!"

    nodes = [f"node-{i}" for i in range(settings.node_replicas)]
    for node_id in nodes:
        manager.add_index_node(
            id=node_id,
            address=f"nohost-{str(uuid.uuid4())}:1234",
            shard_count=0,
            available_disk=100,
            dummy=True,
        )

    yield nodes

    for node_id in nodes:
        manager.remove_index_node(node_id)
