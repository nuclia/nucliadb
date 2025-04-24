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

from abc import ABCMeta
from typing import AsyncIterator

from nidx_protos import nodereader_pb2, noderesources_pb2
from nidx_protos.nodewriter_pb2 import (
    NewShardRequest,
    VectorIndexConfig,
)

from nucliadb_protos import utils_pb2


class AbstractIndexNode(metaclass=ABCMeta):
    label: str = "index-node"

    def __init__(
        self,
        *,
        id: str,
    ):
        self.id = id

    def __str__(self):
        return f"{self.__class__.__name__}({self.id}"

    def __repr__(self):
        return self.__str__()

    async def stream_get_fields(
        self, stream_request: nodereader_pb2.StreamRequest
    ) -> AsyncIterator[nodereader_pb2.DocumentItem]:
        async for idandfacets in self.reader.Documents(stream_request):  # type: ignore
            yield idandfacets

    async def stream_get_paragraphs(
        self, stream_request: nodereader_pb2.StreamRequest
    ) -> AsyncIterator[nodereader_pb2.ParagraphItem]:
        async for idandfacets in self.reader.Paragraphs(stream_request):  # type: ignore
            yield idandfacets

    async def get_shard(self, shard_id: str) -> noderesources_pb2.Shard:
        req = nodereader_pb2.GetShardRequest()
        req.shard_id.id = shard_id
        return await self.reader.GetShard(req)  # type: ignore

    async def new_shard(
        self,
        kbid: str,
        vector_index_config: VectorIndexConfig,
    ) -> noderesources_pb2.ShardCreated:
        req = NewShardRequest(
            kbid=kbid,
            release_channel=utils_pb2.ReleaseChannel.STABLE,
            config=vector_index_config,
            # Deprecated fields, only for backwards compatibility with older nodes
            similarity=vector_index_config.similarity,
            normalize_vectors=vector_index_config.normalize_vectors,
        )

        resp = await self.writer.NewShard(req)  # type: ignore
        return resp
