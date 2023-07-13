import abc
import asyncio
import logging

from nucliadb_models.cluster import ClusterMember

from .. import manager
from ..index_node import IndexNode
from ..settings import Settings

logger = logging.getLogger(__name__)


def update_members(members: list[ClusterMember]) -> None:
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
            logger.debug(f"{member.node_id} add {member.listen_addr}")
            manager.add_index_node(
                IndexNode(
                    id=member.node_id,
                    address=member.listen_addr,
                    shard_count=shard_count,
                )
            )
            logger.debug("Node added")
        else:
            logger.debug(f"{member.node_id} update")
            node.address = member.listen_addr
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


class AbstractPullDiscovery(AbstractClusterDiscovery):
    """ """

    @abc.abstractmethod
    async def discover(self) -> None:
        """
        This method should discover all nodes in the cluster and update them
        """

    async def watch(self) -> None:
        while True:
            try:
                await asyncio.sleep(15)
                await self.discover()
            except asyncio.CancelledError:
                return

    async def initialize(self) -> None:
        await self.discover()
        self.task = asyncio.create_task(self.watch())

    async def finalize(self) -> None:
        self.task.cancel()
