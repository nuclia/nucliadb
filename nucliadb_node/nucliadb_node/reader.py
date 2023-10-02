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
import logging
from typing import Optional

from grpc import StatusCode
from grpc.aio import AioRpcError  # type: ignore
from lru import LRU  # type: ignore
from nucliadb_protos.nodereader_pb2 import GetShardRequest  # type: ignore
from nucliadb_protos.nodereader_pb2_grpc import NodeReaderStub
from nucliadb_protos.noderesources_pb2 import Shard, ShardId
from nucliadb_protos.nodewriter_pb2 import OpStatus

from nucliadb_node import SERVICE_NAME  # type: ignore
from nucliadb_utils.grpc import get_traced_grpc_channel

logger = logging.getLogger(__name__)
CACHE = LRU(128)


class Reader:
    lock: asyncio.Lock

    def __init__(self, grpc_reader_address: str):
        self.lock = asyncio.Lock()
        self.channel = get_traced_grpc_channel(grpc_reader_address, SERVICE_NAME)
        self.stub = NodeReaderStub(self.channel)  # type: ignore

    async def get_shard(self, pb: ShardId) -> Optional[Shard]:
        if pb.id not in CACHE:
            req = GetShardRequest()
            req.shard_id.id = pb.id
            try:
                shard: Shard = await self.stub.GetShard(req)  # type: ignore
            except AioRpcError as exc:
                if exc.code() != StatusCode.NOT_FOUND:
                    raise
                logger.warning("Shard not found", {"shard_id": pb.id})
                return None
            CACHE[pb.id] = shard
            return CACHE[pb.id]
        else:
            return CACHE[pb.id]

    def update(self, shard: str, status: OpStatus):
        if status.status == 0:
            cached_shard = Shard()
            cached_shard.shard_id = shard
            cached_shard.fields = status.field_count
            cached_shard.paragraphs = status.paragraph_count
            cached_shard.sentences = status.sentence_count
            CACHE[shard] = cached_shard

    async def close(self):
        await self.channel.close()
