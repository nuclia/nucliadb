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

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.exceptions import NodeClusterSmall
from nucliadb.common.cluster.index_node import IndexNode
from nucliadb.common.cluster.settings import settings


@pytest.fixture(scope="function")
def available_nodes():
    nodes = {
        "node-0": IndexNode(
            id="node-0", address="node-0", shard_count=1, available_disk=100, dummy=True
        ),
        "node-30": IndexNode(
            id="node-30",
            address="node-30",
            shard_count=1,
            available_disk=30,
            dummy=True,
        ),
        "node-40": IndexNode(
            id="node-40",
            address="node-40",
            shard_count=1,
            available_disk=10,
            dummy=True,
        ),
    }
    with mock.patch.object(manager, "INDEX_NODES", new=nodes):
        yield nodes


def test_sorted_primary_nodes_orders_by_available_disk(available_nodes):
    with mock.patch.object(settings, "node_replicas", 2):
        nodes = manager.sorted_primary_nodes()
        assert nodes == ["node-0", "node-30", "node-40"]


def test_sorted_primary_nodes_puts_nodes_to_avoid_at_the_end(available_nodes):
    with mock.patch.object(settings, "node_replicas", 2):
        excluded_node = "node-0"
        nodes = manager.sorted_primary_nodes(avoid_nodes=[excluded_node])
        assert nodes == ["node-30", "node-40", "node-0"]

        # even if all are used, still should find nodes
        all_nodes = list(available_nodes.keys())
        assert manager.sorted_primary_nodes(avoid_nodes=all_nodes) == [
            "node-0",
            "node-30",
            "node-40",
        ]


def test_check_enough_nodes_raises_error_if_not_enough_nodes_are_found(available_nodes):
    with mock.patch.object(settings, "node_replicas", 200):
        with pytest.raises(NodeClusterSmall):
            manager.check_enough_nodes()


def test_check_enough_nodes_checks_max_node_replicas_only_if_set(available_nodes):
    with mock.patch.object(settings, "max_node_replicas", 0):
        with pytest.raises(NodeClusterSmall):
            manager.check_enough_nodes()

    with mock.patch.object(settings, "max_node_replicas", -1):
        manager.check_enough_nodes()
