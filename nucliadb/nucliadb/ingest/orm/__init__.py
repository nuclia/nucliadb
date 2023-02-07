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
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List

from nucliadb.ingest.orm.exceptions import NodeClusterSmall
from nucliadb.ingest.settings import settings

if TYPE_CHECKING:
    from nucliadb.ingest.orm.node import Node

NODES: Dict[str, Node] = {}


class ClusterObject:
    local_node: Any

    def __init__(self):
        self.local_node = None

    def get_local_node(self):
        return self.local_node

    def find_nodes(self) -> List[str]:
        return self.find_nodes_greedy()

    def find_nodes_greedy(self) -> List[str]:
        node_replicas = settings.node_replicas
        total_nodes = len(NODES)
        if total_nodes < node_replicas:
            raise NodeClusterSmall(
                f"Not enough nodes available reported by chitchat: {total_nodes} vs {node_replicas}"
            )

        # Filter out those over max node shards
        available_nodes = {
            nid: node
            for (nid, node) in NODES.items()
            if node.shard_count < settings.max_node_shards
        }
        if len(available_nodes) < node_replicas:
            raise NodeClusterSmall(
                f"Could not find enough nodes with available shards: {len(available_nodes)} vs {node_replicas}"
            )

        # Sort available nodes by shard_count and load_scode
        sorted_nodes = [
            (nid, node.shard_count, node.load_score)
            for nid, node in available_nodes.items()
        ]
        sorted_nodes = sorted(sorted_nodes, key=lambda x: (x[1], x[2]))

        return [nodeid for nodeid, _, _ in sorted_nodes][:node_replicas]


NODE_CLUSTER = ClusterObject()
