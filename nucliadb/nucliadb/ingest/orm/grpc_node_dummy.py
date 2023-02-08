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
from __future__ import annotations

from typing import Any, Dict, List

from nucliadb_protos.noderesources_pb2 import Shard as NodeResourcesShard
from nucliadb_protos.noderesources_pb2 import (
    ShardCleaned,
    ShardCreated,
    ShardId,
    ShardList,
    VectorSetList,
)
from nucliadb_protos.nodewriter_pb2 import Counter, OpStatus, ShadowShardResponse


class DummyWriterStub:
    calls: Dict[str, List[Any]] = {}

    async def GetShard(self, data):
        self.calls.setdefault("GetShard", []).append(data)
        return NodeResourcesShard(
            shard_id="shard", resources=2, paragraphs=2, sentences=2
        )

    async def NewShard(self, data):
        self.calls.setdefault("NewShard", []).append(data)
        return ShardCreated(id="shard")

    async def DeleteShard(self, data):
        self.calls.setdefault("DeleteShard", []).append(data)
        return ShardId(id="shard")

    async def CleanAndUpgradeShard(self, data):
        self.calls.setdefault("CleanAndUpgradeShard", []).append(data)
        return ShardCleaned(
            document_service=ShardCreated.DocumentService.DOCUMENT_V1,
            paragraph_service=ShardCreated.ParagraphService.PARAGRAPH_V1,
            vector_service=ShardCreated.VectorService.VECTOR_V1,
            relation_service=ShardCreated.RelationService.RELATION_V1,
        )

    async def ListShards(self, data):
        self.calls.setdefault("ListShards", []).append(data)
        sl = ShardList()
        sl.shards.append(NodeResourcesShard(shard_id="shard", resources=2))
        sl.shards.append(NodeResourcesShard(shard_id="shard2", resources=4))
        return sl

    async def SetResource(self, data):
        self.calls.setdefault("SetResource", []).append(data)
        result = OpStatus()
        result.count = 1
        return result

    async def AddVectorSet(self, data):
        self.calls.setdefault("AddVectorSet", []).append(data)
        result = OpStatus()
        result.count = 1
        return result

    async def ListVectorSet(self, data: ShardId):
        self.calls.setdefault("ListVectorSet", []).append(data)
        result = VectorSetList()
        result.shard.id = data.id
        result.vectorset.append("base")
        return result


class DummyReaderStub:
    calls: Dict[str, List[Any]] = {}

    async def GetShard(self, data):
        self.calls.setdefault("GetShard", []).append(data)
        return NodeResourcesShard(
            shard_id="shard", resources=2, paragraphs=2, sentences=2
        )


class DummySidecarStub:
    calls: Dict[str, List[Any]] = {}

    async def GetCount(self, data):
        self.calls.setdefault("GetCount", []).append(data)
        return Counter(paragraphs=2, resources=2)

    async def CreateShadowShard(self, data):
        self.calls.setdefault("CreateShadowShard", []).append(data)
        return ShadowShardResponse(success=True, shard=ShardId(id="shadow"))

    async def DeleteShadowShard(self, data):
        self.calls.setdefault("DeleteShadowShard", []).append(data)
        return ShadowShardResponse(success=True, shard=ShardId(id="shadow"))
