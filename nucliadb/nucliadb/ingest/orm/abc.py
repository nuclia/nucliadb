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
from typing import Any, AsyncIterator, List, Optional

from nucliadb_protos.nodereader_pb2 import GetShardRequest  # type: ignore
from nucliadb_protos.nodereader_pb2 import DocumentItem, ParagraphItem, StreamRequest
from nucliadb_protos.nodereader_pb2_grpc import NodeReaderStub
from nucliadb_protos.noderesources_pb2 import EmptyQuery
from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.noderesources_pb2 import Shard as NodeResourcesShard
from nucliadb_protos.noderesources_pb2 import (
    ShardCleaned,
    ShardCreated,
    ShardId,
    VectorSetID,
    VectorSetList,
)
from nucliadb_protos.nodewriter_pb2 import (
    NewShardRequest,
    NewVectorSetRequest,
    OpStatus,
)
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_protos.writer_pb2 import ShardObject as PBShard
from nucliadb_protos.writer_pb2 import Shards as PBShards
from pydantic import BaseModel

from nucliadb.ingest.maindb.driver import Transaction
from nucliadb_utils.keys import KB_SHARDS


class ShardCounter(BaseModel):
    shard: str
    fields: int
    paragraphs: int


class AbstractShard(metaclass=ABCMeta):
    @abstractmethod
    def __init__(
        self, sharduuid: str, shard: PBShard, node: Optional[Any] = None
    ):  # pragma: no cover
        pass

    @abstractmethod
    async def delete_resource(
        self, uuid: str, txid: int, partition: str, kb: str
    ):  # pragma: no cover
        pass

    @abstractmethod
    async def add_resource(
        self,
        resource: PBBrainResource,
        txid: int,
        partition: str,
        kb: str,
        reindex_id: Optional[str] = None,
    ) -> Optional[ShardCounter]:  # pragma: no cover
        pass


class AbstractNode(metaclass=ABCMeta):
    label: str

    @property
    @abstractmethod
    def reader(self) -> NodeReaderStub:  # pragma: no cover
        pass

    @property
    @abstractmethod
    def writer(self) -> NodeWriterStub:  # pragma: no cover
        pass

    @classmethod
    @abstractmethod
    def create_shard_klass(
        cls, shard_id: str, pbshard: PBShard
    ) -> AbstractShard:  # pragma: no cover
        pass

    @classmethod
    @abstractmethod
    async def create_shard_by_kbid(
        cls, txn: Transaction, kbid: str, similarity: VectorSimilarity.ValueType
    ) -> AbstractShard:  # pragma: no cover
        """
        Create a new shard for a knowledge box and assign the current active
        shard for new resources to be indexed into.
        """
        pass

    @classmethod
    @abstractmethod
    async def get_current_active_shard(
        cls, txn: Transaction, kbid: str
    ) -> Optional[AbstractShard]:  # pragma: no cover
        """
        Shard that is currently receiving new resources
        """
        pass

    async def stream_get_fields(
        self, stream_request: StreamRequest
    ) -> AsyncIterator[DocumentItem]:
        async for idandfacets in self.reader.Documents(stream_request):  # type: ignore
            yield idandfacets

    async def stream_get_paragraphs(
        self, stream_request: StreamRequest
    ) -> AsyncIterator[ParagraphItem]:
        async for idandfacets in self.reader.Paragraphs(stream_request):  # type: ignore
            yield idandfacets

    @classmethod
    async def get_all_shards(cls, txn: Transaction, kbid: str) -> Optional[PBShards]:
        key = KB_SHARDS.format(kbid=kbid)
        kb_shards_bytes: Optional[bytes] = await txn.get(key)
        if kb_shards_bytes is not None:
            kb_shards = PBShards()
            kb_shards.ParseFromString(kb_shards_bytes)
            return kb_shards
        else:
            return None

    async def get_reader_shard(
        self, shard_id: str, vectorset: Optional[str] = None
    ) -> NodeResourcesShard:
        req = GetShardRequest()
        req.shard_id.id = shard_id
        if vectorset is not None:
            req.vectorset = vectorset
        return await self.reader.GetShard(req)  # type: ignore

    async def get_shard(self, id: str) -> ShardId:
        req = ShardId(id=id)
        resp = await self.writer.GetShard(req)  # type: ignore
        return resp

    async def new_shard(
        self,
        kbid: str,
        similarity: VectorSimilarity.ValueType,
    ) -> ShardCreated:
        req = NewShardRequest(kbid=kbid, similarity=similarity)
        resp = await self.writer.NewShard(req)  # type: ignore
        return resp

    async def delete_shard(self, id: str) -> str:
        req = ShardId(id=id)
        resp: ShardId = await self.writer.DeleteShard(req)  # type: ignore
        return resp.id

    async def list_shards(self) -> List[str]:
        req = EmptyQuery()
        resp = await self.writer.ListShards(req)  # type: ignore
        return resp.shards

    async def clean_and_upgrade_shard(self, id: str) -> ShardCleaned:
        req = ShardId(id=id)
        resp = await self.writer.CleanAndUpgradeShard(req)  # type: ignore
        return resp

    async def del_vectorset(self, shard_id: str, vectorset: str) -> OpStatus:
        req = VectorSetID()
        req.shard.id = shard_id
        req.vectorset = vectorset
        resp = await self.writer.RemoveVectorSet(req)  # type: ignore
        return resp

    async def set_vectorset(
        self,
        shard_id: str,
        vectorset: str,
        similarity: VectorSimilarity.ValueType = VectorSimilarity.COSINE,
    ) -> OpStatus:
        req = NewVectorSetRequest()
        req.id.shard.id = shard_id
        req.id.vectorset = vectorset
        req.similarity = similarity
        resp = await self.writer.AddVectorSet(req)  # type: ignore
        return resp

    async def get_vectorset(self, shard_id: str) -> VectorSetList:
        req = ShardId()
        req.id = shard_id
        resp = await self.writer.ListVectorSets(req)  # type: ignore
        return resp
