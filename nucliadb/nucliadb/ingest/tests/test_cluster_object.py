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
    nodes = {}
    for index, load_score in enumerate(range(10)):
        node_id = f"node-{index}"
        nodes[node_id] = Node(node_id, NodeType.IO, load_score, dummy=True)

    with mock.patch.object(orm, "NODES", new=nodes):
        yield


def test_find_nodes(nodes):
    cluster = orm.ClusterObject()
    assert cluster.find_nodes() == ["node-0", "node-1"]


def test_find_nodes_raises_error_if_not_enough_nodes(nodes):
    cluster = orm.ClusterObject()

    prev = settings.node_replicas
    settings.node_replicas = 200

    with pytest.raises(NodeClusterSmall):
        cluster.find_nodes()

    settings.node_replicas = prev
