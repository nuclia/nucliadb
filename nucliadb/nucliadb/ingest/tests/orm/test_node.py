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
import pytest

from nucliadb.ingest.orm import NODES
from nucliadb.ingest.orm.node import ClusterMember, NodeType, chitchat_update_node


def get_cluster_member(
    node_id="foo",
    listen_addr="192.1.1.1:8080",
    type=NodeType.IO,
    online=True,
    is_self=False,
    load_score=0,
) -> ClusterMember:
    return ClusterMember(
        node_id=node_id,
        listen_addr=listen_addr,
        type=type,
        online=online,
        is_self=is_self,
        load_score=load_score,
    )


@pytest.mark.asyncio
async def test_chitchat_update_node():
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
    assert node.load_score == member.load_score

    # Check that it updates loads score for registered members
    member.load_score = 30
    await chitchat_update_node([member])
    assert len(NODES) == 1
    node = NODES["node1"]
    assert node.load_score == 30

    # Check that it removes members that are no longer reported
    await chitchat_update_node([])
    assert len(NODES) == 0
