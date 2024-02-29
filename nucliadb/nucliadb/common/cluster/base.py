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

from abc import ABCMeta, abstractmethod
from typing import AsyncIterator, Optional

from nucliadb_protos.nodereader_pb2_grpc import NodeReaderStub
from nucliadb_protos.nodewriter_pb2 import (
    NewShardRequest,
    NewVectorSetRequest,
    OpStatus,
)
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub

from nucliadb_protos import nodereader_pb2, noderesources_pb2, utils_pb2


class AbstractIndexNode(metaclass=ABCMeta):
    label: str = "index-node"

    def __init__(
        self,
        *,
        id: str,
        address: str,
        shard_count: int,
        available_disk: int,
        dummy: bool = False,
        primary_id: Optional[str] = None,
    ):
        self.id = id
        self.address = address
        self.shard_count = shard_count
        self.available_disk = available_disk
        self.dummy = dummy
        self.primary_id = primary_id

    def __str__(self):
        if self.primary_id is None:
            return f"{self.__class__.__name__}({self.id}, {self.address})"
        else:
            return f"{self.__class__.__name__}({self.id}, {self.address}, primary_id={self.primary_id})"

    def __repr__(self):
        return self.__str__()

    def is_read_replica(self) -> bool:
        return self.primary_id is not None

    @property
    @abstractmethod
    def reader(self) -> NodeReaderStub:  # pragma: no cover
        pass

    @property
    @abstractmethod
    def writer(self) -> NodeWriterStub:  # pragma: no cover
        pass

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

    async def get_shard(
        self, shard_id: str, vectorset: Optional[str] = None
    ) -> noderesources_pb2.Shard:
        req = nodereader_pb2.GetShardRequest()
        req.shard_id.id = shard_id
        if vectorset is not None:
            req.vectorset = vectorset
        return await self.reader.GetShard(req)  # type: ignore

    async def new_shard(
        self,
        kbid: str,
        similarity: utils_pb2.VectorSimilarity.ValueType,
        release_channel: utils_pb2.ReleaseChannel.ValueType,
    ) -> noderesources_pb2.ShardCreated:
        req = NewShardRequest(
            kbid=kbid, similarity=similarity, release_channel=release_channel
        )
        resp = await self.writer.NewShard(req)  # type: ignore
        return resp

    async def list_shards(self) -> list[str]:
        shards = await self.writer.ListShards(noderesources_pb2.EmptyQuery())  # type: ignore
        return [shard.id for shard in shards.ids]

    async def delete_shard(self, id: str) -> str:
        req = noderesources_pb2.ShardId(id=id)
        resp: noderesources_pb2.ShardId = await self.writer.DeleteShard(req)  # type: ignore
        return resp.id

    async def del_vectorset(self, shard_id: str, vectorset: str) -> OpStatus:
        req = noderesources_pb2.VectorSetID()
        req.shard.id = shard_id
        req.vectorset = vectorset
        resp = await self.writer.RemoveVectorSet(req)  # type: ignore
        return resp

    async def set_vectorset(
        self,
        shard_id: str,
        vectorset: str,
        similarity: utils_pb2.VectorSimilarity.ValueType = utils_pb2.VectorSimilarity.COSINE,
    ) -> OpStatus:
        req = NewVectorSetRequest()
        req.id.shard.id = shard_id
        req.id.vectorset = vectorset
        req.similarity = similarity
        resp = await self.writer.AddVectorSet(req)  # type: ignore
        return resp

    async def get_vectorset(self, shard_id: str) -> noderesources_pb2.VectorSetList:
        req = noderesources_pb2.ShardId()
        req.id = shard_id
        resp = await self.writer.ListVectorSets(req)  # type: ignore
        return resp
