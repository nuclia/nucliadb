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
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Type, TypeVar, Union

from google.protobuf.json_format import MessageToDict
from nucliadb_protos.nodereader_pb2 import OrderBy
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject
from pydantic import BaseModel

from nucliadb.ingest.serialize import ExtractedDataTypeName, ResourceProperties
from nucliadb.models.common import FieldTypeName
from nucliadb.models.resource import Resource

if TYPE_CHECKING:
    SortValue = OrderBy.OrderType.V
else:
    SortValue = int

_T = TypeVar("_T")


class NucliaDBRoles(str, Enum):
    MANAGER = "MANAGER"
    READER = "READER"
    WRITER = "WRITER"


class SearchOptions(str, Enum):
    PARAGRAPH = "paragraph"
    DOCUMENT = "document"
    RELATIONS = "relations"
    VECTOR = "vector"


class SuggestOptions(str, Enum):
    PARAGRAPH = "paragraph"
    ENTITIES = "entities"
    INTENT = "intent"


class NucliaDBClientType(str, Enum):
    API = "api"
    WIDGET = "widget"
    WEB = "web"
    DASHBOARD = "dashboard"
    DESKTOP = "desktop"
    CHROME_EXTENSION = "chrome_extension"


class Sort(int, Enum):
    DESC = 0
    ASC = 1


class Facet(BaseModel):
    facetresults: Dict[str, int]


FacetsResult = Dict[str, Any]


class Sentence(BaseModel):
    score: float
    rid: str
    text: str
    field_type: str
    field: str
    labels: List[str] = []


class Sentences(BaseModel):
    results: List[Sentence] = []
    facets: FacetsResult
    page_number: int = 0
    page_size: int = 20


class ParagraphPosition(BaseModel):
    page_number: Optional[int]
    index: int
    start: int
    end: int


class Paragraph(BaseModel):
    score: float
    rid: str
    field_type: str
    field: str
    text: str
    labels: List[str] = []
    start_seconds: Optional[List[int]] = None
    end_seconds: Optional[List[int]] = None
    position: Optional[ParagraphPosition] = None


class Paragraphs(BaseModel):
    results: List[Paragraph] = []
    facets: Optional[FacetsResult] = None
    query: Optional[str] = None
    total: int = 0
    page_number: int = 0
    page_size: int = 20
    next_page: bool = False


class ResourceResult(BaseModel):
    score: Union[float, int]
    rid: str
    field_type: str
    field: str


class Resources(BaseModel):
    results: List[ResourceResult]
    facets: Optional[FacetsResult] = None
    query: Optional[str] = None
    total: int = 0
    page_number: int = 0
    page_size: int = 20
    next_page: bool = False


class Relation(BaseModel):
    title: str
    uri: str
    resources: List[ResourceResult]


class Relations(BaseModel):
    results: List[Relation] = []


class RelatedEntities(BaseModel):
    total: int = 0
    entities: List[str] = []


class ResourceSearchResults(BaseModel):
    sentences: Optional[Sentences] = None
    paragraphs: Optional[Paragraphs] = None
    relations: Optional[Relations] = None
    nodes: Optional[List[Tuple[str, str, str]]]
    shards: Optional[List[str]]


class KnowledgeboxSearchResults(BaseModel):
    resources: Dict[str, Resource] = {}
    sentences: Optional[Sentences] = None
    paragraphs: Optional[Paragraphs] = None
    fulltext: Optional[Resources] = None
    relations: Optional[Relations] = None
    nodes: Optional[List[Tuple[str, str, str]]]
    shards: Optional[List[str]]


class KnowledgeboxSuggestResults(BaseModel):
    paragraphs: Optional[Paragraphs] = None
    entities: Optional[RelatedEntities] = None
    shards: Optional[List[Tuple[str, str, str]]]


class KnowledgeboxCounters(BaseModel):
    resources: int
    paragraphs: int
    fields: int
    sentences: int
    shards: Optional[List[Tuple[str, str, str]]]


class SortOption(str, Enum):
    MODIFIED = "modified"
    CREATED = "created"


class KnowledgeBoxCount(BaseModel):
    paragraphs: int
    fields: int
    sentences: int


class DocumentServiceEnum(str, Enum):
    DOCUMENT_V0 = "DOCUMENT_V0"
    DOCUMENT_V1 = "DOCUMENT_V1"


class ParagraphServiceEnum(str, Enum):
    PARAGRAPH_V0 = "PARAGRAPH_V0"
    PARAGRAPH_V1 = "PARAGRAPH_V1"


class VectorServiceEnum(str, Enum):
    VECTOR_V0 = "VECTOR_V0"
    VECTOR_V1 = "VECTOR_V1"


class RelationServiceEnum(str, Enum):
    RELATION_V0 = "RELATION_V0"
    RELATION_V1 = "RELATION_V1"


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
    replicas: List[ShardReplica]

    @classmethod
    def from_message(cls: Type[_T], message: PBShardObject) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


class KnowledgeboxShards(BaseModel):
    kbid: str
    actual: int
    shards: List[ShardObject]


class SearchRequest(BaseModel):
    query: str
    fields: List[str] = []
    filters: List[str] = []
    faceted: List[str] = []
    sort: Optional[SortOption] = None
    page_number: int = 0
    page_size: int = 20
    min_score: float = 0.70
    range_creation_start: Optional[datetime] = None
    range_creation_end: Optional[datetime] = None
    range_modification_start: Optional[datetime] = None
    range_modification_end: Optional[datetime] = None
    features: List[SearchOptions] = [
        SearchOptions.PARAGRAPH,
        SearchOptions.DOCUMENT,
        SearchOptions.VECTOR,
        SearchOptions.RELATIONS,
    ]
    reload: bool = True
    debug: bool = False
    highlight: bool = False
    show: List[ResourceProperties] = [ResourceProperties.BASIC]
    field_type_filter: List[FieldTypeName] = list(FieldTypeName)
    extracted: List[ExtractedDataTypeName] = list(ExtractedDataTypeName)
    shards: List[str] = []
    vector: Optional[List[float]] = None
    with_duplicates: bool = False
