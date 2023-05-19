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
from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.settings import settings
from nucliadb_models.cluster import MemberType
from nucliadb_utils.keys import KB_SHARDS


def test_member_type_from_str():
    for raw_type, member_type in [
        ("Io", MemberType.IO),
        ("Train", MemberType.TRAIN),
        ("Ingest", MemberType.INGEST),
        ("Search", MemberType.SEARCH),
    ]:
        assert MemberType(raw_type) == member_type


def test_member_type_pb_conversion():
    for member_type, pb_member_type in [
        (MemberType.IO, Member.Type.IO),
        (MemberType.TRAIN, Member.Type.TRAIN),
        (MemberType.INGEST, Member.Type.INGEST),
        (MemberType.SEARCH, Member.Type.SEARCH),
        (MemberType.UNKNOWN, Member.Type.UNKNOWN),
    ]:
        assert member_type.to_pb() == pb_member_type
        assert MemberType.from_pb(pb_member_type) == member_type


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
        type=MemberType.IO,
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


def test_node_str():
    node = Node(
        id="node-1", address="host:1234", type=MemberType.IO, shard_count=0, dummy=True
    )
    assert str(node) == repr(node) == "Node(node-1, host:1234)"
