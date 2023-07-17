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
import abc
import asyncio
import logging

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.discovery.types import IndexNodeMetadata
from nucliadb.common.cluster.index_node import IndexNode
from nucliadb.common.cluster.settings import Settings
from nucliadb_telemetry import metrics

logger = logging.getLogger(__name__)

AVAILABLE_NODES = metrics.Gauge("nucliadb_nodes_available")


def update_members(members: list[IndexNodeMetadata]) -> None:
    # First add new nodes or update existing ones
    valid_ids = []
    for member in members:
        valid_ids.append(member.node_id)

        shard_count = member.shard_count
        if shard_count is None:
            shard_count = 0
            logger.warning(f"Node {member.node_id} has no shard_count")

        node = manager.get_index_node(member.node_id)
        if node is None:
            logger.debug(f"{member.node_id} add {member.address}")
            manager.add_index_node(
                IndexNode(
                    id=member.node_id,
                    address=member.address,
                    shard_count=shard_count,
                )
            )
            logger.debug("Node added")
        else:
            logger.debug(f"{member.node_id} update")
            node.address = member.address
            node.shard_count = shard_count
            logger.debug("Node updated")

    # Then cleanup nodes that are no longer reported
    node_ids = [x.id for x in manager.get_index_nodes()]
    removed_node_ids = []
    for key in node_ids:
        if key not in valid_ids:
            node = manager.get_index_node(key)
            if node is not None:
                removed_node_ids.append(key)
                logger.warning(f"{key} remove {node.address}")
                manager.remove_index_node(key)

    if len(removed_node_ids) > 1:
        logger.warning(
            f"{len(removed_node_ids)} nodes are down simultaneously. This should never happen!"
        )

    AVAILABLE_NODES.set(len(manager.get_index_nodes()))


class AbstractClusterDiscovery(abc.ABC):
    task: asyncio.Task

    def __init__(self, settings: Settings):
        self.settings = settings

    @abc.abstractmethod
    async def initialize(self) -> None:
        """ """

    @abc.abstractmethod
    async def finalize(self) -> None:
        """ """
