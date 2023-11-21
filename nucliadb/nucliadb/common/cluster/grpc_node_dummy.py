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
from typing import Any, Dict, List

from nucliadb_protos.nodereader_pb2 import (
    EdgeList,
    RelationEdge,
    RelationSearchResponse,
    TypeList,
)
from nucliadb_protos.noderesources_pb2 import EmptyResponse
from nucliadb_protos.noderesources_pb2 import Shard as NodeResourcesShard
from nucliadb_protos.noderesources_pb2 import (
    ShardCleaned,
    ShardCreated,
    ShardId,
    ShardIds,
    VectorSetList,
)
from nucliadb_protos.nodewriter_pb2 import OpStatus
from nucliadb_protos.utils_pb2 import Relation


class DummyWriterStub:  # pragma: no cover
    calls: Dict[str, List[Any]] = {}

    async def NewShard(self, data):  # pragma: no cover
        self.calls.setdefault("NewShard", []).append(data)
        return ShardCreated(id="shard")

    async def DeleteShard(self, data):  # pragma: no cover
        self.calls.setdefault("DeleteShard", []).append(data)
        return ShardId(id="shard")

    async def CleanAndUpgradeShard(self, data):  # pragma: no cover
        self.calls.setdefault("CleanAndUpgradeShard", []).append(data)
        return ShardCleaned(
            document_service=ShardCreated.DocumentService.DOCUMENT_V1,
            paragraph_service=ShardCreated.ParagraphService.PARAGRAPH_V1,
            vector_service=ShardCreated.VectorService.VECTOR_V1,
            relation_service=ShardCreated.RelationService.RELATION_V1,
        )

    async def ListShards(self, data):  # pragma: no cover
        self.calls.setdefault("ListShards", []).append(data)
        shards = ShardIds()
        shards.append(ShardId(shard_id="shard"))
        shards.append(ShardId(shard_id="shard2"))
        return shards

    async def SetResource(self, data):  # pragma: no cover
        self.calls.setdefault("SetResource", []).append(data)
        result = OpStatus()
        result.field_count = 1
        return result

    async def AddVectorSet(self, data):  # pragma: no cover
        self.calls.setdefault("AddVectorSet", []).append(data)
        result = OpStatus()
        result.field_count = 1
        return result

    async def ListVectorSet(self, data: ShardId):  # pragma: no cover
        self.calls.setdefault("ListVectorSet", []).append(data)
        result = VectorSetList()
        result.shard.id = data.id
        result.vectorset.append("base")
        return result

    async def GC(self, request: ShardId) -> EmptyResponse:  # pragma: no cover
        self.calls.setdefault("GC", []).append(request)
        return EmptyResponse()


class DummyReaderStub:  # pragma: no cover
    calls: Dict[str, List[Any]] = {}

    async def GetShard(self, data):  # pragma: no cover
        self.calls.setdefault("GetShard", []).append(data)
        return NodeResourcesShard(shard_id="shard", fields=2, paragraphs=2, sentences=2)

    async def RelationSearch(self, data):  # pragma: no cover
        self.calls.setdefault("RelationSearch", []).append(data)
        result = RelationSearchResponse()
        return result

    async def RelationEdges(self, data):  # pragma: no cover
        self.calls.setdefault("RelationEdges", []).append(data)
        result = EdgeList()
        result.list.append(
            RelationEdge(edge_type=Relation.RelationType.ENTITY, property="dummy")
        )
        return result

    async def RelationTypes(self, data):  # pragma: no cover
        self.calls.setdefault("RelationTypes", []).append(data)
        result = TypeList()
        return result
