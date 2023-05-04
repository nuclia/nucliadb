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

from nucliadb.ingest import orm
from nucliadb.ingest.orm.exceptions import NodeClusterSmall
from nucliadb.ingest.orm.node import Node, NodeType
from nucliadb.ingest.settings import settings


@pytest.fixture(scope="function")
def nodes():
    nodes = {
        "node-30": Node("node-30", NodeType.IO, shard_count=30, dummy=True),
        "node-40": Node("node-40", NodeType.IO, shard_count=40, dummy=True),
        "node-0": Node("node-0", NodeType.IO, shard_count=0, dummy=True),
    }
    with mock.patch.object(orm, "NODES", new=nodes):
        yield nodes


def test_find_nodes_orders_by_shard_count(nodes):
    cluster = orm.ClusterObject()
    nodes_found = cluster.find_nodes()
    assert len(nodes_found) == settings.node_replicas
    assert nodes_found == ["node-0", "node-30"]


def test_find_nodes_exclude_nodes(nodes):
    cluster = orm.ClusterObject()
    excluded_node = "node-0"
    nodes_found = cluster.find_nodes(exclude_nodes=[excluded_node])
    assert nodes_found == ["node-30", "node-40"]

    with pytest.raises(NodeClusterSmall):
        all_nodes = list(nodes.keys())
        cluster.find_nodes(exclude_nodes=all_nodes)


def test_find_nodes_raises_error_if_not_enough_nodes_are_found(nodes):
    cluster = orm.ClusterObject()

    prev = settings.node_replicas
    settings.node_replicas = 200

    with pytest.raises(NodeClusterSmall):
        cluster.find_nodes()

    settings.node_replicas = prev


def test_find_nodes_checks_max_node_replicas_only_if_set(nodes):
    cluster = orm.ClusterObject()

    prev = settings.max_node_replicas
    settings.max_node_replicas = 0

    with pytest.raises(NodeClusterSmall):
        cluster.find_nodes()

    settings.max_node_replicas = -1
    assert len(cluster.find_nodes())

    settings.max_node_replicas = prev
