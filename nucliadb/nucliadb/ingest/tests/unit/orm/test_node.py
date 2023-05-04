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
from unittest import mock

import pytest
from nucliadb_protos.writer_pb2 import Member
from nucliadb_protos.writer_pb2 import Shards as PBShards

from nucliadb.ingest.orm import NODES, NodeClusterSmall
from nucliadb.ingest.orm.node import ClusterMember, Node, NodeType, chitchat_update_node
from nucliadb.ingest.settings import settings
from nucliadb_utils.keys import KB_SHARDS


def get_cluster_member(
    node_id="foo",
    listen_addr="192.1.1.1:8080",
    type=NodeType.IO,
    is_self=False,
    shard_count=0,
) -> ClusterMember:
    return ClusterMember(
        node_id=node_id,
        listen_addr=listen_addr,
        type=type,
        is_self=is_self,
        shard_count=shard_count,
    )


@pytest.mark.asyncio
async def test_chitchat_update_node():
    NODES.clear()
    assert NODES == {}
    await chitchat_update_node([])
    assert len(NODES) == 0

    # Check that it ignores itself
    member = get_cluster_member(is_self=True)
    await chitchat_update_node([member])
    assert len(NODES) == 0

    # Check that it ignores types other than node_type=NodeType.IO
    member = get_cluster_member(type=NodeType.INGEST)
    await chitchat_update_node([member])
    assert len(NODES) == 0

    # Check it registers new members
    member = get_cluster_member(node_id="node1")
    await chitchat_update_node([member])
    assert len(NODES) == 1
    node = NODES["node1"]
    assert node.address == member.listen_addr
    assert node.shard_count == member.shard_count

    # Check that it updates loads score for registered members
    member.shard_count = 2
    await chitchat_update_node([member])
    assert len(NODES) == 1
    node = NODES["node1"]
    assert node.shard_count == 2

    # Check that it removes members that are no longer reported
    await chitchat_update_node([])
    assert len(NODES) == 0


def test_node_type_from_str():
    for raw_type, node_type in [
        ("Io", NodeType.IO),
        ("Train", NodeType.TRAIN),
        ("Ingest", NodeType.INGEST),
        ("Search", NodeType.SEARCH),
        ("Blablabla", NodeType.UNKNOWN),
        ("Cat is everything", NodeType.UNKNOWN),
    ]:
        assert NodeType.from_str(raw_type) == node_type


def test_node_type_pb_conversion():
    for node_type, member_type in [
        (NodeType.IO, Member.Type.IO),
        (NodeType.TRAIN, Member.Type.TRAIN),
        (NodeType.INGEST, Member.Type.INGEST),
        (NodeType.SEARCH, Member.Type.SEARCH),
        (NodeType.UNKNOWN, Member.Type.UNKNOWN),
    ]:
        assert node_type.to_pb() == member_type
        assert NodeType.from_pb(member_type) == node_type


@pytest.mark.asyncio
async def test_update_node_metrics(metrics_registry):
    node1 = "node-1"
    member1 = get_cluster_member(node_id=node1, type=NodeType.IO, shard_count=2)
    await chitchat_update_node([member1])

    assert metrics_registry.get_sample_value("nucliadb_nodes_available", {}) == 1
    assert (
        metrics_registry.get_sample_value("nucliadb_node_shard_count", {"node": node1})
        == 2
    )

    node2 = "node-2"
    member2 = get_cluster_member(node_id=node2, type=NodeType.IO, shard_count=1)
    await chitchat_update_node([member2])

    assert metrics_registry.get_sample_value("nucliadb_nodes_available", {}) == 1
    assert (
        metrics_registry.get_sample_value("nucliadb_node_shard_count", {"node": node2})
        == 1
    )

    # Check that samples of destroyed node have been removed
    assert (
        metrics_registry.get_sample_value("nucliadb_node_shard_count", {"node": node1})
        is None
    )

    NODES.clear()


async def get_kb_shards(txn, kbid):
    kb_shards = None
    kb_shards_key = KB_SHARDS.format(kbid=kbid)
    kb_shards_binary = await txn.get(kb_shards_key)
    if kb_shards_binary:
        kb_shards = PBShards()
        kb_shards.ParseFromString(kb_shards_binary)
    return kb_shards


@pytest.fixture(scope="function")
def one_replica():
    prev = settings.node_replicas
    settings.node_replicas = 1
    yield
    settings.node_replicas = prev


@pytest.mark.asyncio
async def test_create_shard_by_kbid(one_replica, txn, fake_node):
    kbid = "mykbid"
    # Initially there is no shards object
    assert await get_kb_shards(txn, kbid) is None

    # Create a shard
    await Node.create_shard_by_kbid(txn, kbid)

    # Check that kb shards object is correct
    kb_shards = await get_kb_shards(txn, kbid)
    assert len(kb_shards.shards) == 1
    assert kb_shards.actual == 0
    assert len(kb_shards.shards[0].replicas) == 1
    node = kb_shards.shards[0].replicas[0].node

    # Create another shard
    await Node.create_shard_by_kbid(txn, kbid)

    # Check that kb shards object was updated correctly
    kb_shards = await get_kb_shards(txn, kbid)
    assert len(kb_shards.shards) == 2
    assert kb_shards.actual == 1
    assert len(kb_shards.shards[1].replicas) == 1
    # New shard has been created in a different node
    assert kb_shards.shards[1].replicas[0].node != node

    # Attempting to create another shard should fail
    # because we don't have sufficient nodes
    with pytest.raises(NodeClusterSmall):
        await Node.create_shard_by_kbid(txn, kbid)


@pytest.mark.asyncio
async def test_create_shard_by_kbid_insufficient_nodes(txn):
    with pytest.raises(NodeClusterSmall):
        await Node.create_shard_by_kbid(txn, "foo")


@pytest.fixture(scope="function")
async def node_errors():
    id, node = NODES.popitem()
    await Node.set(
        id,
        address="nohost:9999",
        type=NodeType.IO,
        shard_count=0,
        dummy=True,
    )
    NODES[id].new_shard = mock.AsyncMock(side_effect=ValueError)

    yield

    NODES[id] = node


@pytest.mark.asyncio
async def test_create_shard_by_kbid_rolls_back(txn, fake_node, node_errors):
    with pytest.raises(ValueError):
        await Node.create_shard_by_kbid(txn, "foo")
