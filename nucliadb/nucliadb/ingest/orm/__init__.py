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

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from nucliadb.ingest.orm.exceptions import NodeClusterSmall
from nucliadb.ingest.settings import settings
from nucliadb_utils.clandestined import Cluster  # type: ignore

if TYPE_CHECKING:
    from nucliadb.ingest.orm.node import Node

NODES: Dict[str, Node] = {}


@dataclass
class ScoredNode:
    id: str
    shard_count: int
    load_score: float


class ClusterObject:
    date: datetime
    cluster: Optional[Cluster] = None
    local_node: Any

    def __init__(self):
        self.local_node = None
        self.date = datetime.now()

    def get_local_node(self):
        return self.local_node

    def find_nodes(self, exclude_nodes: Optional[List[str]] = None) -> List[str]:
        """
        Returns a list of node ids sorted by increasing shard count and load score.
        It will exclude the node ids in `excluded_nodes` from the computation.
        It raises an exception if it can't find enough nodes for the configured replicas.
        """
        target_replicas = settings.node_replicas
        available_nodes = [
            ScoredNode(
                id=node_id, shard_count=node.shard_count, load_score=node.load_score
            )
            for node_id, node in NODES.items()
        ]
        if len(available_nodes) < target_replicas:
            raise NodeClusterSmall(
                f"Not enough nodes. Total: {len(available_nodes)}, Required: {target_replicas}"
            )

        if exclude_nodes:
            available_nodes = list(
                filter(lambda x: x.id not in exclude_nodes, available_nodes)  # type: ignore
            )
            if len(available_nodes) < target_replicas:
                raise NodeClusterSmall(
                    f"Could not find enough nodes. Available: {len(available_nodes)}, Required: {target_replicas}"  # noqa
                )

        if settings.max_node_shards >= 0:
            available_nodes = list(
                filter(
                    lambda x: x.shard_count < settings.max_node_shards, available_nodes  # type: ignore
                )
            )
            if len(available_nodes) < target_replicas:
                raise NodeClusterSmall(
                    f"Could not find enough nodes with available shards. Available: {len(available_nodes)}, Required: {target_replicas}"  # noqa
                )

        # Sort available nodes by increasing shard_count and load_scode
        sorted_nodes = sorted(
            available_nodes, key=lambda x: (x.shard_count, x.load_score)
        )
        return [node.id for node in sorted_nodes][:target_replicas]


NODE_CLUSTER = ClusterObject()
