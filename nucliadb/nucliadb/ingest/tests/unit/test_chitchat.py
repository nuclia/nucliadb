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

import pytest

from nucliadb.ingest.chitchat import ChitchatMonitor, update_available_nodes
from nucliadb.ingest.orm.node import NODES
from nucliadb_models.cluster import ClusterMember, MemberType


@pytest.fixture(scope="function")
def asyncio_mock():
    task = unittest.mock.MagicMock()
    with unittest.mock.patch(
        "nucliadb.ingest.chitchat.asyncio.create_task", return_value=task
    ) as mock:
        yield mock


def get_cluster_member(
    node_id="foo",
    listen_addr="192.1.1.1:8080",
    type=MemberType.IO,
    online=True,
    is_self=False,
    load_score=0,
    shard_count=0,
) -> ClusterMember:
    return ClusterMember(
        node_id=node_id,
        listen_addr=listen_addr,
        type=type,
        online=online,
        is_self=is_self,
        load_score=load_score,
        shard_count=shard_count,
    )


@pytest.mark.asyncio
async def test_chitchat_monitor(asyncio_mock):
    chitchat = ChitchatMonitor("127.0.0.1", 8888)
    await chitchat.start()
    assert chitchat.task is not None
    await chitchat.finalize()
    chitchat.task.cancel.assert_called_once()


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

    # Check that it ignores types other than node_type=MemberType  .IO
    member = get_cluster_member(type=MemberType.INGEST)
    await update_available_nodes([member])
    assert len(NODES) == 0

    # Check it registers new members
    member = get_cluster_member(node_id="node1")
    await update_available_nodes([member])
    assert len(NODES) == 1
    node = NODES["node1"]
    assert node.address == member.listen_addr
    assert node.load_score == member.load_score
    assert node.shard_count == member.shard_count

    # Check that it updates loads score for registered members
    member.load_score = 30
    member.shard_count = 2
    await update_available_nodes([member])
    assert len(NODES) == 1
    node = NODES["node1"]
    assert node.load_score == 30
    assert node.shard_count == 2

    # Check that it removes members that are no longer reported
    await update_available_nodes([])
    assert len(NODES) == 0


@pytest.mark.asyncio
async def test_update_node_metrics(metrics_registry):
    node1 = "node-1"
    member1 = get_cluster_member(
        node_id=node1, type=MemberType.IO, load_score=10, shard_count=2
    )
    await update_available_nodes([member1])

    assert metrics_registry.get_sample_value("nucliadb_nodes_available", {}) == 1
    assert (
        metrics_registry.get_sample_value("nucliadb_node_shard_count", {"node": node1})
        == 2
    )
    assert (
        metrics_registry.get_sample_value("nucliadb_node_load_score", {"node": node1})
        == 10
    )

    node2 = "node-2"
    member2 = get_cluster_member(
        node_id=node2, type=MemberType.IO, load_score=40, shard_count=1
    )
    await update_available_nodes([member2])

    assert metrics_registry.get_sample_value("nucliadb_nodes_available", {}) == 1
    assert (
        metrics_registry.get_sample_value("nucliadb_node_shard_count", {"node": node2})
        == 1
    )
    assert (
        metrics_registry.get_sample_value("nucliadb_node_load_score", {"node": node2})
        == 40
    )

    # Check that samples of destroyed node have been removed
    assert (
        metrics_registry.get_sample_value("nucliadb_node_shard_count", {"node": node1})
        is None
    )
    assert (
        metrics_registry.get_sample_value("nucliadb_node_load_score", {"node": node1})
        is None
    )

    NODES.clear()
