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
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nucliadb_protos.noderesources_pb2 import ShardId
from nucliadb_protos.utils_pb2 import RelationNode

from nucliadb.common.cluster.settings import Settings
from nucliadb.common.cluster.standalone import service
from nucliadb_protos import nodereader_pb2, standalone_pb2

pytestmark = pytest.mark.asyncio


@pytest.fixture
def cluster_settings():
    settings = Settings()
    with patch(
        "nucliadb.common.cluster.standalone.service.cluster_settings", settings
    ), tempfile.TemporaryDirectory() as tmpdir:
        settings.data_path = tmpdir
        os.makedirs(os.path.join(tmpdir, "shards"))
        yield settings


@pytest.fixture
def self_node(cluster_settings):
    self_node = MagicMock(id="id", address="address", shard_count=0, available_disk=10)
    self_node.reader = AsyncMock()
    self_node.writer = AsyncMock()
    self_node.reader.Search.return_value = nodereader_pb2.SearchResponse()

    relation_edges = nodereader_pb2.EdgeList()
    edge = nodereader_pb2.RelationEdge(
        edge_type=RelationNode.NodeType.ENTITY, property="foo"
    )
    relation_edges.list.append(edge)
    self_node.reader.RelationEdges.return_value = relation_edges

    with patch("nucliadb.common.cluster.standalone.service.get_self") as mock_get_self:
        mock_get_self.return_value = self_node
        yield self_node


@pytest.fixture
def servicer(self_node):
    yield service.StandaloneClusterServiceServicer()


async def test_node_action(
    servicer: service.StandaloneClusterServiceServicer,
    self_node,
    cluster_settings,
):
    resp = await servicer.NodeAction(
        standalone_pb2.NodeActionRequest(
            service="reader",
            action="Search",
            payload=nodereader_pb2.SearchRequest(body="test").SerializeToString(),
        ),
        None,
    )
    assert resp == standalone_pb2.NodeActionResponse(
        payload=self_node.reader.Search.return_value.SerializeToString()
    )


async def test_reader_node_action_relations(
    servicer: service.StandaloneClusterServiceServicer,
    self_node,
    cluster_settings,
):
    resp = await servicer.NodeAction(
        standalone_pb2.NodeActionRequest(
            service="reader",
            action="RelationEdges",
            payload=ShardId(id="test").SerializeToString(),
        ),
        None,
    )
    assert resp == standalone_pb2.NodeActionResponse(
        payload=self_node.reader.RelationEdges.return_value.SerializeToString()
    )


async def test_node_info(
    servicer: service.StandaloneClusterServiceServicer, self_node, cluster_settings
):
    resp = await servicer.NodeInfo(standalone_pb2.NodeInfoRequest(), None)
    assert resp.id == self_node.id
    assert resp.address == self_node.address
    assert resp.shard_count == self_node.shard_count
    assert resp.available_disk > 0
