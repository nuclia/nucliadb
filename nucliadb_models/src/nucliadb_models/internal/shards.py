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
