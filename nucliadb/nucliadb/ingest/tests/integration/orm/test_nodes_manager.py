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

import pytest
from nucliadb_protos.writer_pb2 import ShardCreated, ShardObject, ShardReplica, Shards

from nucliadb.ingest.maindb.driver import Driver
from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.orm.nodes_manager import NodesManager
from nucliadb_models.cluster import MemberType
from nucliadb_utils.keys import KB_SHARDS


@pytest.fixture(scope="function")
async def fake_kbid() -> AsyncIterable[str]:
    yield uuid.uuid4().hex


@pytest.fixture(scope="function")
async def fake_nodes():
    await Node.set(
        "node-0",
        address="nohost:9999",
        type=MemberType.IO,
        shard_count=0,
        dummy=True,
    )
    await Node.set(
        "node-1",
        address="nohost:9999",
        type=MemberType.IO,
        shard_count=0,
        dummy=True,
    )
    await Node.set(
        "node-2",
        address="nohost:9999",
        type=MemberType.IO,
        shard_count=0,
        dummy=True,
    )
    yield


@pytest.fixture(scope="function")
async def shards(fake_nodes, fake_kbid: str, redis_driver: Driver):
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
    driver = redis_driver
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
async def test_choose_node(shards, redis_driver: Driver, shard_index: int, nodes: set):
    driver = redis_driver

    nodes_manager = NodesManager(driver=driver)
    shard = shards.shards[shard_index]
    node_ids = set()
    for i in range(100):
        _, _, node_id = nodes_manager.choose_node(shard)
        node_ids.add(node_id)
    assert node_ids == nodes, "Random numbers have defeat this test"


@pytest.mark.asyncio
async def test_apply_for_all_shards(fake_kbid: str, shards, redis_driver: Driver):
    kbid = fake_kbid
    driver = redis_driver

    nodes_manager = NodesManager(driver=driver)

    nodes = []

    async def fun(node: Node, shard_id: str, node_id: str):
        nodes.append((shard_id, node_id))

    await nodes_manager.apply_for_all_shards(kbid, fun, timeout=10)

    nodes.sort()
    assert len(nodes) == 2
    assert nodes[0] == ("shard-a.1", "node-0") or nodes[0] == ("shard-a.2", "node-1")
    assert nodes[1] == ("shard-b.1", "node-1") or nodes[1] == ("shard-b.2", "node-2")
