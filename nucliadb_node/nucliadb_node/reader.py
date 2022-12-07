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

import asyncio
from typing import Optional

from grpc import aio  # type: ignore
from lru import LRU  # type: ignore
from nucliadb_protos.nodereader_pb2 import GetShardRequest  # type: ignore
from nucliadb_protos.nodereader_pb2_grpc import NodeReaderStub
from nucliadb_protos.noderesources_pb2 import Shard, ShardId
from nucliadb_protos.nodewriter_pb2 import OpStatus

from nucliadb_node import SERVICE_NAME  # type: ignore
from nucliadb_telemetry.grpc import OpenTelemetryGRPC
from nucliadb_telemetry.utils import get_telemetry

CACHE = LRU(128)


class Reader:
    _stub: Optional[NodeReaderStub] = None
    lock: asyncio.Lock

    def __init__(self, grpc_reader_address: str):
        self.lock = asyncio.Lock()
        tracer_provider = get_telemetry(SERVICE_NAME)
        if tracer_provider is not None:
            telemetry_grpc = OpenTelemetryGRPC(
                f"{SERVICE_NAME}_grpc_reader", tracer_provider
            )
            self.channel = telemetry_grpc.init_client(grpc_reader_address)
        else:
            self.channel = aio.insecure_channel(grpc_reader_address)
        self.stub = NodeReaderStub(self.channel)

    async def get_count(self, pb: ShardId) -> Optional[int]:
        if pb.id not in CACHE:
            req = GetShardRequest()
            req.shard_id.id = pb.id
            shard: Shard = await self.stub.GetShard(req)  # type: ignore
            CACHE[pb.id] = shard.resources
            return CACHE[pb.id]
        else:
            return CACHE[pb.id]

    def update(self, shard: str, status: OpStatus):
        if status.status == 0:
            CACHE[shard] = status.count
