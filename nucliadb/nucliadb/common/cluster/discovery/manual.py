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
import asyncio
import logging

from nucliadb_protos.noderesources_pb2 import EmptyQuery

from nucliadb.common.cluster.discovery.abc import (
    AbstractClusterDiscovery,
    update_members,
)
from nucliadb.common.cluster.discovery.types import IndexNodeMetadata
from nucliadb_protos import nodewriter_pb2, nodewriter_pb2_grpc
from nucliadb_utils.grpc import get_traced_grpc_channel

logger = logging.getLogger(__name__)


class ManualDiscovery(AbstractClusterDiscovery):
    """
    Manual provide all cluster members addresses to load information from.
    """

    async def discover(self) -> None:
        members = []
        for address in self.settings.cluster_discovery_manual_addresses:
            grpc_address = f"{address}:{self.settings.node_writer_port}"
            channel = get_traced_grpc_channel(
                grpc_address, "discovery", variant="_writer"
            )
            stub = nodewriter_pb2_grpc.NodeWriterStub(channel)
            metadata: nodewriter_pb2.NodeMetadata = await stub.GetMetadata(EmptyQuery())  # type: ignore
            members.append(
                IndexNodeMetadata(
                    node_id=metadata.node_id,
                    name=metadata.node_id,
                    address=address,
                    shard_count=metadata.shard_count,
                )
            )
        update_members(members)

    async def watch(self) -> None:
        while True:
            try:
                await asyncio.sleep(15)
                await self.discover()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception(
                    "Error while watching cluster members. Will retry at started interval"
                )

    async def initialize(self) -> None:
        await self.discover()
        self.task = asyncio.create_task(self.watch())

    async def finalize(self) -> None:
        self.task.cancel()
