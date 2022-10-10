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

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

from clandestined import Cluster  # type: ignore

from nucliadb.ingest.orm.exceptions import NodeClusterNotFound, NodeClusterSmall
from nucliadb.ingest.settings import settings
from nucliadb_utils.settings import nuclia_settings

if TYPE_CHECKING:
    from nucliadb.ingest.orm.node import Node

NODES: Dict[str, Node] = {}


class ClusterObject:
    date: datetime
    cluster: Optional[Cluster] = None
    local_node: Any

    def __init__(self):
        self.local_node = None
        self.date = datetime.now()

    def get_local_node(self):
        return self.local_node

    def find_nodes(self, kbid: str):
        if self.cluster is not None:
            nodes = self.cluster.find_nodes(kbid)
            if len(nodes) != len(set(nodes)):
                raise NodeClusterSmall()
            if len(nodes) < settings.node_replicas:
                raise NodeClusterSmall()

            return nodes[: settings.node_replicas]
        else:
            raise NodeClusterNotFound()

    def compute(self):
        self.date = datetime.now()
        if len(NODES) == 0:
            self.cluster = None
            return

        cluster_info = {}
        for count, (id, node) in enumerate(NODES.items()):
            cluster_info[id] = {
                "name": node.address,
                "label": node.label,
                "zone": f"z{count % settings.node_replicas}",
            }
            count += 1

        self.cluster = Cluster(
            cluster_info,
            replicas=settings.node_replicas,
            seed=nuclia_settings.nuclia_hash_seed,
        )


NODE_CLUSTER = ClusterObject()
