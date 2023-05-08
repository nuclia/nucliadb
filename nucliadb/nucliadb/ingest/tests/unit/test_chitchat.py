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
import unittest
from unittest.mock import AsyncMock

import pytest

from nucliadb.ingest.chitchat import ChitchatMonitor, update_available_nodes
from nucliadb.ingest.orm.node import NODES
from nucliadb_models.cluster import ClusterMember, MemberType


@pytest.fixture(scope="function")
def start_server():
    task = unittest.mock.MagicMock()
    with unittest.mock.patch(
        "nucliadb.ingest.chitchat.start_server", return_value=task
    ) as mock:
        yield mock


def get_cluster_member(
    node_id="foo",
    listen_addr="192.1.1.1:8080",
    type=MemberType.IO,
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
async def test_chitchat_monitor(start_server):
    chitchat = ChitchatMonitor("127.0.0.1", 8888)
    await chitchat.start()
    chitchat.server = AsyncMock()
    await chitchat.finalize()
    chitchat.server.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_available_nodes():
    NODES.clear()
    assert NODES == {}
    await update_available_nodes([])
    assert len(NODES) == 0

    # Check that it ignores itself
    member = get_cluster_member(is_self=True)
    await update_available_nodes([member])
    assert len(NODES) == 0

    # Check that it ignores types other than node_type=MemberType.IO
    member = get_cluster_member(type=MemberType.INGEST)
    await update_available_nodes([member])
    assert len(NODES) == 0

    # Check it registers new members
    member = get_cluster_member(node_id="node1")
    await update_available_nodes([member])
    assert len(NODES) == 1
    node = NODES["node1"]
    assert node.address == member.listen_addr

    assert node.shard_count == member.shard_count

    # Check that it updates loads score for registered members
    member.shard_count = 2
    member.listen_addr = "0.0.0.0:7777"
    member2 = get_cluster_member(node_id="node2", shard_count=1)
    await update_available_nodes([member, member2])
    assert len(NODES) == 2
    node = NODES["node1"]
    assert node.shard_count == 2
    assert node.address == "0.0.0.0:7777"
    node2 = NODES["node2"]
    assert node2.shard_count == 1

    # Check that it removes members that are no longer reported
    await update_available_nodes([])
    assert len(NODES) == 0


@pytest.mark.asyncio
async def test_update_node_metrics(metrics_registry):
    node1 = "node-1"
    member1 = get_cluster_member(node_id=node1, type=MemberType.IO, shard_count=2)
    await update_available_nodes([member1])

    assert metrics_registry.get_sample_value("nucliadb_nodes_available", {}) == 1
    assert (
        metrics_registry.get_sample_value("nucliadb_node_shard_count", {"node": node1})
        == 2
    )

    node2 = "node-2"
    member2 = get_cluster_member(node_id=node2, type=MemberType.IO, shard_count=1)
    await update_available_nodes([member2])

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
