# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class DocumentServiceEnum(str, Enum):
    DOCUMENT_V0 = "DOCUMENT_V0"
    DOCUMENT_V1 = "DOCUMENT_V1"
    DOCUMENT_V2 = "DOCUMENT_V2"
    DOCUMENT_V3 = "DOCUMENT_V3"


class ParagraphServiceEnum(str, Enum):
    PARAGRAPH_V0 = "PARAGRAPH_V0"
    PARAGRAPH_V1 = "PARAGRAPH_V1"
    PARAGRAPH_V2 = "PARAGRAPH_V2"
    PARAGRAPH_V3 = "PARAGRAPH_V3"


class VectorServiceEnum(str, Enum):
    VECTOR_V0 = "VECTOR_V0"
    VECTOR_V1 = "VECTOR_V1"


class RelationServiceEnum(str, Enum):
    RELATION_V0 = "RELATION_V0"
    RELATION_V1 = "RELATION_V1"
    RELATION_V2 = "RELATION_V2"


class ShardCreated(BaseModel):
    id: str
    document_service: DocumentServiceEnum
    paragraph_service: ParagraphServiceEnum
    vector_service: VectorServiceEnum
    relation_service: RelationServiceEnum


class ShardReplica(BaseModel):
    node: str
    shard: ShardCreated


class ShardObject(BaseModel):
    shard: str
    nidx_shard_id: Optional[str]


class KnowledgeboxShards(BaseModel):
    kbid: str
    shards: List[ShardObject]
