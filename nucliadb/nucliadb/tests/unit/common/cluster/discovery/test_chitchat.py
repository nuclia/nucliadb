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

from nucliadb.common.cluster.discovery.abc import update_members
from nucliadb.common.cluster.discovery.chitchat import ChitchatAutoDiscovery
from nucliadb.common.cluster.discovery.types import IndexNodeMetadata
from nucliadb.common.cluster.manager import INDEX_NODES
from nucliadb.common.cluster.settings import Settings


@pytest.fixture(scope="function")
def start_server():
    task = unittest.mock.MagicMock()
    with unittest.mock.patch(
        "nucliadb.common.cluster.discovery.chitchat.start_server", return_value=task
    ) as mock:
        yield mock


def get_cluster_member(
    node_id="foo",
    listen_addr="192.1.1.1:8080",
    shard_count=0,
) -> IndexNodeMetadata:
    return IndexNodeMetadata(
        node_id=node_id, address=listen_addr, shard_count=shard_count, name="name"
    )


@pytest.mark.asyncio
async def test_chitchat_monitor(start_server):
    chitchat = ChitchatAutoDiscovery(
        Settings(chitchat_binding_host="127.0.0.1", chitchat_binding_port=8888)
    )
    await chitchat.initialize()
    chitchat.server = AsyncMock()
    await chitchat.finalize()
    chitchat.server.shutdown.assert_awaited_once()


@pytest.mark.asyncio
async def test_update_available_nodes():
    INDEX_NODES.clear()
    assert INDEX_NODES == {}
    update_members([])
    assert len(INDEX_NODES) == 0

    # Check it registers new members
    member = get_cluster_member(node_id="node1")
    update_members([member])
    assert len(INDEX_NODES) == 1
    node = INDEX_NODES["node1"]
    assert node.address == member.address

    assert node.shard_count == member.shard_count

    # Check that it updates loads score for registered members
    member.shard_count = 2
    member.address = "0.0.0.0:7777"
    member2 = get_cluster_member(node_id="node2", shard_count=1)
    update_members([member, member2])
    assert len(INDEX_NODES) == 2
    node = INDEX_NODES["node1"]
    assert node.shard_count == 2
    assert node.address == "0.0.0.0:7777"
    node2 = INDEX_NODES["node2"]
    assert node2.shard_count == 1

    # Check that it removes members that are no longer reported
    update_members([])
    assert len(INDEX_NODES) == 0
