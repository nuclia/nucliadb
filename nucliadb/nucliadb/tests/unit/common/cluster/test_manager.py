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
from unittest import mock
from unittest.mock import MagicMock

import pytest

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.exceptions import NoHealthyNodeAvailable
from nucliadb.common.cluster.settings import settings
from nucliadb_protos import writer_pb2


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


def add_index_node(id: str):
    manager.add_index_node(
        id=id,
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
    )


def add_read_replica_node(id: str, primary_id: str):
    manager.add_index_node(
        id=id,
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
        primary_id=primary_id,
    )


def test_choose_node_with_two_primary_nodes():
    manager.INDEX_NODES.clear()
    add_index_node("node-0")
    add_index_node("node-1")

    node, _ = manager.choose_node(
        writer_pb2.ShardObject(
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-0"
                )
            ]
        )
    )
    assert node.id == "node-0"
    node, _ = manager.choose_node(
        writer_pb2.ShardObject(
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-1"
                )
            ]
        )
    )
    assert node.id == "node-1"

    manager.INDEX_NODES.clear()


def test_choose_node_with_two_read_replicas():
    """Test choose_node with two replica nodes pointing to two different primary
    nodes.

    """
    manager.INDEX_NODES.clear()
    add_read_replica_node("node-replica-0", primary_id="node-0")
    add_read_replica_node("node-replica-1", primary_id="node-1")

    node, _ = manager.choose_node(
        writer_pb2.ShardObject(
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-0"
                )
            ]
        ),
        use_read_replica_nodes=True,
    )
    assert node.id == "node-replica-0"
    node, _ = manager.choose_node(
        writer_pb2.ShardObject(
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-1"
                )
            ]
        ),
        use_read_replica_nodes=True,
    )
    assert node.id == "node-replica-1"

    manager.INDEX_NODES.clear()


def test_choose_node_no_healthy_node_available():
    """There's only one read replica for node-0 and we try to choose a node for
    a shard in node-1. We expect it to fail as there's no possible valid node to
    choose.

    """
    manager.INDEX_NODES.clear()
    add_read_replica_node("node-replica-0", primary_id="node-0")

    with pytest.raises(NoHealthyNodeAvailable):
        manager.choose_node(
            writer_pb2.ShardObject(
                replicas=[
                    writer_pb2.ShardReplica(
                        shard=writer_pb2.ShardCreated(id="123"), node="node-1"
                    )
                ]
            ),
            use_read_replica_nodes=True,
        )

    manager.INDEX_NODES.clear()


def repeated_choose_node(
    count: int, shard: writer_pb2.ShardObject, **kwargs
) -> tuple[list[str], list[str]]:
    shard_ids = []
    node_ids = []

    for _ in range(count):
        node, shard_id = manager.choose_node(shard, **kwargs)
        shard_ids.append(shard_id)
        node_ids.append(node.id)

    return shard_ids, node_ids


def test_choose_node_with_nodes_and_replicas(standalone_mode_off):
    """Validate how choose node selects between different options depending on
    configuration.

    As some choices can be random between a subset of nodes, choose_node is
    called multiple times per assert.

    """
    TRIES_PER_ASSERT = 10

    shard = writer_pb2.ShardObject(
        replicas=[
            writer_pb2.ShardReplica(
                shard=writer_pb2.ShardCreated(id="123"),
                node="node-0",
            ),
            writer_pb2.ShardReplica(
                shard=writer_pb2.ShardCreated(id="456"),
                node="node-1",
            ),
        ]
    )

    # Start with 2 nodes and 1 read replica each
    manager.INDEX_NODES.clear()
    add_index_node("node-0")
    add_index_node("node-1")
    add_read_replica_node("node-replica-0", primary_id="node-0")
    add_read_replica_node("node-replica-1", primary_id="node-1")

    # Without read replicas, we only choose primaries
    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT, shard, use_read_replica_nodes=False
    )
    assert set(shard_ids) == {"123", "456"}
    assert set(node_ids) == {"node-0", "node-1"}

    # Secondaries are preferred
    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT, shard, use_read_replica_nodes=True
    )
    assert set(shard_ids) == {"123", "456"}
    assert set(node_ids) == {"node-replica-0", "node-replica-1"}

    # Target replicas take more preference
    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT,
        shard,
        use_read_replica_nodes=False,
        target_shard_replicas=["456"],
    )
    assert set(shard_ids) == {"456"}
    assert set(node_ids) == {"node-1"}

    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT,
        shard,
        use_read_replica_nodes=True,
        target_shard_replicas=["456"],
    )
    assert set(shard_ids) == {"456"}
    assert set(node_ids) == {"node-replica-1"}

    # Let's remove a node so it becomes unavailable, replica keeps working
    manager.INDEX_NODES.clear()
    add_index_node("node-0")
    add_read_replica_node("node-replica-0", primary_id="node-0")
    add_read_replica_node("node-replica-1", primary_id="node-1")

    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT, shard, use_read_replica_nodes=False
    )
    assert set(shard_ids) == {"123"}
    assert set(node_ids) == {"node-0"}

    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT, shard, use_read_replica_nodes=True
    )
    assert set(shard_ids) == {"123", "456"}
    assert set(node_ids) == {"node-replica-0", "node-replica-1"}

    # target replicas is ignored but only primaries are used
    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT,
        shard,
        use_read_replica_nodes=False,
        target_shard_replicas=["456"],
    )
    assert set(shard_ids) == {"123"}
    assert set(node_ids) == {"node-0"}

    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT,
        shard,
        use_read_replica_nodes=True,
        target_shard_replicas=["456"],
    )
    assert set(shard_ids) == {"456"}
    assert set(node_ids) == {"node-replica-1"}

    # Now let's add again the node but remove the replica
    manager.INDEX_NODES.clear()
    add_index_node("node-0")
    add_index_node("node-1")
    add_read_replica_node("node-replica-0", primary_id="node-0")

    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT, shard, use_read_replica_nodes=False
    )
    assert set(shard_ids) == {"123", "456"}
    assert set(node_ids) == {"node-0", "node-1"}

    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT, shard, use_read_replica_nodes=True
    )
    assert set(shard_ids) == {"123"}
    assert set(node_ids) == {"node-replica-0"}

    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT,
        shard,
        use_read_replica_nodes=False,
        target_shard_replicas=["456"],
    )
    assert set(shard_ids) == {"456"}
    assert set(node_ids) == {"node-1"}

    shard_ids, node_ids = repeated_choose_node(
        TRIES_PER_ASSERT,
        shard,
        use_read_replica_nodes=True,
        target_shard_replicas=["456"],
    )
    assert set(shard_ids) == {"456"}
    assert set(node_ids) == {"node-1"}

    manager.INDEX_NODES.clear()


@pytest.fixture(scope="function")
def standalone_mode_off():
    prev = settings.standalone_mode
    settings.standalone_mode = False
    yield
    settings.standalone_mode = prev


@pytest.fixture(scope="function")
def index_nodes():
    index_nodes = {}
    with mock.patch.object(manager, "INDEX_NODES", new=index_nodes):
        yield index_nodes


def test_get_index_nodes(standalone_mode_off, index_nodes):
    # Add a primary node
    manager.add_index_node(
        id="node-0",
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
    )
    # Add a secondary replica of node-0
    manager.add_index_node(
        id="node-1",
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
        primary_id="node-0",
    )

    # By default, only primary nodes are returned
    nodes = manager.get_index_nodes()
    assert len(nodes) == 1
    assert nodes[0].id == "node-0"

    # If we ask for secondary, we get both
    nodes = manager.get_index_nodes(include_secondary=True)
    assert len(nodes) == 2
    sorted(nodes, key=lambda x: x.id)
    assert nodes[0].id == "node-0"
    assert nodes[1].id == "node-1"
