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

import backoff

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.discovery.types import IndexNodeMetadata
from nucliadb.common.cluster.settings import Settings
from nucliadb_protos import (
    noderesources_pb2,
    nodewriter_pb2,
    nodewriter_pb2_grpc,
    replication_pb2_grpc,
    standalone_pb2,
    standalone_pb2_grpc,
)
from nucliadb_telemetry import metrics
from nucliadb_utils.grpc import get_traced_grpc_channel

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
                id=member.node_id,
                address=member.address,
                shard_count=shard_count,
                primary_id=member.primary_id,
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
                manager.remove_index_node(key, node.primary_id)

    if len(removed_node_ids) > 1:
        logger.warning(
            f"{len(removed_node_ids)} nodes are down simultaneously. This should never happen!"
        )

    AVAILABLE_NODES.set(len(manager.get_index_nodes()))


@backoff.on_exception(backoff.expo, (Exception,), max_tries=4)
async def _get_index_node_metadata(
    settings: Settings, address: str, read_replica: bool = False
) -> IndexNodeMetadata:
    """
    Get node metadata directly from the writer.

    Establishes a new connection on every try on purpose to avoid long lived connections
    and dns caching issues.

    This method should be used carefully and results should be cached.
    """
    if address in settings.writer_port_map:
        # test wiring
        port = settings.writer_port_map[address]
        grpc_address = f"localhost:{port}"
    else:
        grpc_address = f"{address}:{settings.node_writer_port}"
    channel = get_traced_grpc_channel(grpc_address, "discovery", variant="_writer")
    if read_replica:
        # on a read replica, we need to use the replication service
        stub = replication_pb2_grpc.ReplicationServiceStub(channel)  # type: ignore
    else:
        stub = nodewriter_pb2_grpc.NodeWriterStub(channel)  # type: ignore
    metadata: nodewriter_pb2.NodeMetadata = await stub.GetMetadata(noderesources_pb2.EmptyQuery())  # type: ignore
    primary_id = (
        getattr(metadata, "primary_node_id", None)
        # the or None here is important because the proto returns an empty string
        or None
    )
    if read_replica and primary_id is None:
        raise Exception(
            "Primary node id not found when it is expected to be a read replica"
        )
    return IndexNodeMetadata(
        node_id=metadata.node_id,
        name=metadata.node_id,
        address=address,
        shard_count=metadata.shard_count,
        primary_id=primary_id,
    )


@backoff.on_exception(backoff.expo, (Exception,), max_tries=4)
async def _get_standalone_index_node_metadata(
    settings: Settings, address: str
) -> IndexNodeMetadata:
    if ":" not in address:
        grpc_address = f"{address}:{settings.standalone_node_port}"
    else:
        grpc_address = address
    channel = get_traced_grpc_channel(grpc_address, "standalone_proxy")
    stub = standalone_pb2_grpc.StandaloneClusterServiceStub(channel)  # type: ignore
    resp: standalone_pb2.NodeInfoResponse = await stub.NodeInfo(standalone_pb2.NodeInfoRequest())  # type: ignore
    return IndexNodeMetadata(
        node_id=resp.id,
        name=resp.id,
        address=address,
        shard_count=resp.shard_count,
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

    async def _query_node_metadata(
        self, address: str, read_replica: bool = False
    ) -> IndexNodeMetadata:
        if self.settings.standalone_mode:
            return await _get_standalone_index_node_metadata(self.settings, address)
        else:
            return await _get_index_node_metadata(self.settings, address, read_replica)
