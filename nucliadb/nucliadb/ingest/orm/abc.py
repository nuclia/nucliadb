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
from typing import Any, AsyncIterator, Optional

from nucliadb_protos.nodereader_pb2 import DocumentItem, ParagraphItem, StreamRequest
from nucliadb_protos.noderesources_pb2 import Resource as PBBrainResource
from nucliadb_protos.writer_pb2 import ShardObject as PBShard

from nucliadb.ingest.maindb.driver import Transaction


class AbstractShard(metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, sharduuid: str, shard: PBShard, node: Optional[Any] = None):
        pass

    @abstractmethod
    async def delete_resource(self, uuid: str, txid: int):
        pass

    @abstractmethod
    async def add_resource(
        self, resource: PBBrainResource, txid: int, reindex_id: Optional[str] = None
    ) -> int:
        pass


class AbstractNode(metaclass=ABCMeta):
    label: str
    reader: Any

    @classmethod
    @abstractmethod
    def create_shard_klass(cls, shard_id: str, pbshard: PBShard) -> AbstractShard:
        pass

    @classmethod
    @abstractmethod
    async def create_shard_by_kbid(cls, txn: Transaction, kbid: str) -> AbstractShard:
        pass

    @classmethod
    @abstractmethod
    async def actual_shard(cls, txn: Transaction, kbid: str) -> Optional[AbstractShard]:
        pass

    @abstractmethod
    async def del_vectorset(self, shard: str, id: str):
        pass

    @abstractmethod
    async def set_vectorset(self, shard: str, id: str):
        pass

    async def stream_get_fields(
        self, stream_request: StreamRequest
    ) -> AsyncIterator[DocumentItem]:
        async for idandfacets in self.reader.Documents(stream_request=stream_request):
            yield idandfacets

    async def stream_get_paragraphs(
        self, stream_request: StreamRequest
    ) -> AsyncIterator[ParagraphItem]:
        async for idandfacets in self.reader.Paragraphs(stream_request=stream_request):
            yield idandfacets
