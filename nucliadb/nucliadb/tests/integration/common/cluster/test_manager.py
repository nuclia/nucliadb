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
import uuid
from typing import AsyncIterable
from unittest import mock

import pytest
from nucliadb_protos.writer_pb2 import ShardCreated, ShardObject, ShardReplica, Shards

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.exceptions import (
    ExhaustedNodesError,
    NoHealthyNodeAvailable,
)
from nucliadb.common.datamanagers.cluster import KB_SHARDS
from nucliadb.common.maindb.driver import Driver


@pytest.fixture(scope="function")
async def fake_kbid() -> AsyncIterable[str]:
    yield uuid.uuid4().hex


@pytest.fixture(scope="function")
async def fake_nodes():
    manager.INDEX_NODES.clear()
    manager.add_index_node(
        id="node-0",
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
    )
    manager.add_index_node(
        id="node-1",
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
    )
    manager.add_index_node(
        id="node-2",
        address="nohost",
        shard_count=0,
        available_disk=100,
        dummy=True,
    )
    yield


@pytest.fixture(scope="function")
async def shards(fake_nodes, fake_kbid: str, maindb_driver: Driver):
    """
    Shards:
    - shard-a
    - shard-b

    ShardReplicas:
    - shard-a.1 -> node-0
    - shard-a.2 -> node-1
    - shard-b.1 -> node-1
    - shard-b.2 -> node-2
    """
    driver = maindb_driver
    kbid = fake_kbid

    shards = Shards()
    shards.kbid = kbid

    shard_a = ShardObject()
    shard_a.shard = "shard-a"
    shard_a.replicas.extend(
        [
            ShardReplica(
                node="node-0",
                shard=ShardCreated(id="shard-a.1"),
            ),
            ShardReplica(
                node="node-1",
                shard=ShardCreated(id="shard-a.2"),
            ),
        ]
    )

    shard_b = ShardObject()
    shard_b.shard = "shard-b"
    shard_b.replicas.extend(
        [
            ShardReplica(
                node="node-1",
                shard=ShardCreated(id="shard-b.1"),
            ),
            ShardReplica(
                node="node-2",
                shard=ShardCreated(id="shard-b.2"),
            ),
        ]
    )

    shards.shards.append(shard_a)
    shards.shards.append(shard_b)

    key = KB_SHARDS.format(kbid=kbid)

    async with driver.transaction() as txn:
        await txn.set(key, shards.SerializeToString())
        await txn.commit()

    yield shards

    async with driver.transaction() as txn:
        await txn.delete(key)
        await txn.commit()


@pytest.mark.parametrize(
    "shard_index,nodes",
    [
        (0, {"node-0", "node-1"}),
        (1, {"node-1", "node-2"}),
    ],
)
@pytest.mark.asyncio
async def test_choose_node(shards, shard_index: int, nodes: set):
    shard = shards.shards[shard_index]
    node_ids = set()
    for i in range(100):
        node, _ = manager.choose_node(shard)
        node_ids.add(node.id)
    assert node_ids == nodes, "Random numbers have defeat this test"


async def test_choose_node_attempts_target_replicas_but_is_not_imperative(shards):
    shard = shards.shards[0]
    r0 = shard.replicas[0].shard.id
    n0 = shard.replicas[0].node
    r1 = shard.replicas[1].shard.id
    n1 = shard.replicas[1].node

    node, replica_id = manager.choose_node(shard, target_shard_replicas=[r0])
    assert replica_id == r0
    assert node.id == n0

    # Change the node-0 to a non-existent node id in order to
    # test the target_shard_replicas logic is not imperative
    shard.replicas[0].node = "I-do-not-exist"
    node, replica_id = manager.choose_node(shard, target_shard_replicas=[r0])
    assert replica_id == r1
    assert node.id == n1


async def test_choose_node_raises_if_no_nodes(shards):
    # Override the node ids to non-existent nodes
    shard = shards.shards[0]
    shard.replicas[0].node = "foo"
    shard.replicas[1].node = "bar"

    with pytest.raises(NoHealthyNodeAvailable):
        manager.choose_node(shard)


@pytest.mark.asyncio
async def test_apply_for_all_shards(fake_kbid: str, shards, maindb_driver: Driver):
    kbid = fake_kbid

    shard_manager = manager.KBShardManager()

    nodes = []

    async def fun(node: AbstractIndexNode, shard_id: str):
        nodes.append((shard_id, node.id))

    await shard_manager.apply_for_all_shards(kbid, fun, timeout=10)

    nodes.sort()
    assert len(nodes) == 2
    assert nodes[0] == ("shard-a.1", "node-0") or nodes[0] == ("shard-a.2", "node-1")
    assert nodes[1] == ("shard-b.1", "node-1") or nodes[1] == ("shard-b.2", "node-2")


@pytest.fixture(scope="function")
def node_new_shard():
    with mock.patch(
        "nucliadb.common.cluster.base.AbstractIndexNode.new_shard",
        side_effect=Exception(),
    ) as mocked:
        yield mocked


@pytest.mark.parametrize("release_channel", ("EXPERIMENTAL", "STABLE"))
async def test_create_shard_by_kbid_attempts_on_all_nodes(
    shards, maindb_driver, fake_kbid, node_new_shard, release_channel
):
    shard_manager = manager.KBShardManager()
    async with maindb_driver.transaction() as txn:
        with pytest.raises(ExhaustedNodesError):
            await shard_manager.create_shard_by_kbid(
                txn,
                fake_kbid,
                semantic_model=mock.MagicMock(),
                release_channel=release_channel,
            )

    assert node_new_shard.await_count == len(manager.get_index_nodes())
