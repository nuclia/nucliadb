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
        sorted_nodes = [(nodeid, node.load_score) for nodeid, node in NODES.items()]
        sorted_nodes.sort(key=lambda x: x[1])
        if len(sorted_nodes) < settings.node_replicas:
            raise NodeClusterSmall()
        return [nodeid for nodeid, _ in sorted_nodes][: settings.node_replicas]


NODE_CLUSTER = ClusterObject()
