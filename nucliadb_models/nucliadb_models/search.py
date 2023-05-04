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
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type, TypeVar, Union

from google.protobuf.json_format import MessageToDict
from nucliadb_protos.audit_pb2 import ClientType
from nucliadb_protos.nodereader_pb2 import DocumentScored, OrderBy
from nucliadb_protos.nodereader_pb2 import ParagraphResult as PBParagraphResult
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject
from nucliadb_protos.writer_pb2 import Shards as PBShards
from pydantic import BaseModel, Field, validator

from nucliadb_models.common import FieldTypeName
from nucliadb_models.metadata import RelationType, ResourceProcessingStatus
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.vectors import VectorSimilarity

_T = TypeVar("_T")


class ResourceProperties(str, Enum):
    BASIC = "basic"
    ORIGIN = "origin"
    RELATIONS = "relations"
    VALUES = "values"
    EXTRACTED = "extracted"
    ERRORS = "errors"


class SearchOptions(str, Enum):
    PARAGRAPH = "paragraph"
    DOCUMENT = "document"
    RELATIONS = "relations"
    VECTOR = "vector"


class ChatOptions(str, Enum):
    PARAGRAPHS = "paragraphs"
    RELATIONS = "relations"


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

    def to_proto(self) -> int:
        return ClientType.Value(self.name)


class Sort(int, Enum):
    DESC = 0
    ASC = 1


class Facet(BaseModel):
    facetresults: Dict[str, int]


FacetsResult = Dict[str, Any]


class TextPosition(BaseModel):
    page_number: Optional[int]
    index: int
    start: int
    end: int
    start_seconds: Optional[List[int]] = None
    end_seconds: Optional[List[int]] = None


class Sentence(BaseModel):
    score: float
    rid: str
    text: str
    field_type: str
    field: str
    index: Optional[str] = None
    position: Optional[TextPosition] = None


class Sentences(BaseModel):
    results: List[Sentence] = []
    facets: FacetsResult
    page_number: int = 0
    page_size: int = 20


class Paragraph(BaseModel):
    score: float
    rid: str
    field_type: str
    field: str
    text: str
    labels: List[str] = []
    start_seconds: Optional[List[int]] = None
    end_seconds: Optional[List[int]] = None
    position: Optional[TextPosition] = None


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


class RelationDirection(str, Enum):
    IN = "in"
    OUT = "out"


class EntityType(str, Enum):
    ENTITY = "entity"
    LABEL = "label"
    RESOURCE = "resource"
    USER = "user"


RelationNodeTypeMap = {
    RelationNode.NodeType.ENTITY: EntityType.ENTITY,
    RelationNode.NodeType.LABEL: EntityType.LABEL,
    RelationNode.NodeType.RESOURCE: EntityType.RESOURCE,
    RelationNode.NodeType.USER: EntityType.USER,
}


class DirectionalRelation(BaseModel):
    entity: str
    entity_type: EntityType
    relation: RelationType
    relation_label: str
    direction: RelationDirection


class EntitySubgraph(BaseModel):
    related_to: List[DirectionalRelation]


# TODO: uncomment and implement (next iteration)
# class RelationPath(BaseModel):
#     origin: str
#     destination: str
#     path: List[DirectionalRelation]


class Relations(BaseModel):
    entities: Dict[str, EntitySubgraph]
    # TODO: implement in the next iteration of knowledge graph search
    # graph: List[RelationPath]


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
    shards: Optional[List[str]]


class KnowledgeboxCounters(BaseModel):
    resources: int
    paragraphs: int
    fields: int
    sentences: int
    shards: Optional[List[str]]


class SortField(str, Enum):
    SCORE = "score"
    CREATED = "created"
    MODIFIED = "modified"
    TITLE = "title"


SortFieldMap = {
    SortField.SCORE: None,
    SortField.CREATED: OrderBy.OrderField.CREATED,
    SortField.MODIFIED: OrderBy.OrderField.MODIFIED,
    SortField.TITLE: None,
}


class SortOrder(str, Enum):
    ASC = "asc"
    DESC = "desc"


SortOrderMap = {
    SortOrder.ASC: Sort.ASC,
    SortOrder.DESC: Sort.DESC,
}


class SortOptions(BaseModel):
    field: SortField
    limit: Optional[int] = Field(None, gt=0)
    order: SortOrder = SortOrder.DESC


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
    similarity: VectorSimilarity
    shards: List[ShardObject]

    @classmethod
    def from_message(cls: Type[_T], message: PBShards) -> _T:
        as_dict = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        as_dict["similarity"] = VectorSimilarity.from_message(message.similarity)
        return cls(**as_dict)


class SearchRequest(BaseModel):
    query: str = ""
    advanced_query: Optional[str] = None
    fields: List[str] = []
    filters: List[str] = []
    faceted: List[str] = []
    sort: Optional[SortOptions] = None
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
    ]
    reload: bool = True
    debug: bool = False
    highlight: bool = False
    show: List[ResourceProperties] = [ResourceProperties.BASIC]
    field_type_filter: List[FieldTypeName] = list(FieldTypeName)
    extracted: List[ExtractedDataTypeName] = list(ExtractedDataTypeName)
    shards: List[str] = []
    vector: Optional[List[float]] = None
    vectorset: Optional[str] = None
    with_duplicates: bool = False
    with_status: Optional[ResourceProcessingStatus] = None
    with_synonyms: bool = False


class Author(str, Enum):
    NUCLIA = "NUCLIA"
    USER = "USER"


class Message(BaseModel):
    author: Author
    text: str


class ChatModel(BaseModel):
    question: str
    user_id: str
    retrieval: bool = True
    system: Optional[str] = None
    context: List[Message] = []


class RephraseModel(BaseModel):
    question: str
    context: List[Message] = []
    user_id: str


class ChatRequest(BaseModel):
    query: str = ""
    fields: List[str] = []
    filters: List[str] = []
    min_score: float = 0.70
    features: List[ChatOptions] = [
        ChatOptions.PARAGRAPHS,
        ChatOptions.RELATIONS,
    ]
    range_creation_start: Optional[datetime] = None
    range_creation_end: Optional[datetime] = None
    range_modification_start: Optional[datetime] = None
    range_modification_end: Optional[datetime] = None
    show: List[ResourceProperties] = [ResourceProperties.BASIC]
    field_type_filter: List[FieldTypeName] = list(FieldTypeName)
    extracted: List[ExtractedDataTypeName] = list(ExtractedDataTypeName)
    shards: List[str] = []
    context: Optional[List[Message]] = None


class FindRequest(SearchRequest):
    features: List[SearchOptions] = [
        SearchOptions.PARAGRAPH,
        SearchOptions.VECTOR,
    ]

    @validator("features")
    def fulltext_not_supported(cls, v):
        if SearchOptions.DOCUMENT in v or SearchOptions.DOCUMENT == v:
            raise ValueError("fulltext search not supported")
        return v


class SCORE_TYPE(str, Enum):
    VECTOR = "VECTOR"
    BM25 = "BM25"
    BOTH = "BOTH"


class FindTextPosition(BaseModel):
    page_number: Optional[int]
    start_seconds: Optional[List[int]] = None
    end_seconds: Optional[List[int]] = None
    index: int
    start: int
    end: int


class FindParagraph(BaseModel):
    score: float
    score_type: SCORE_TYPE
    order: int = Field(0, ge=0)
    text: str
    id: str
    labels: Optional[List[str]] = []
    position: Optional[TextPosition] = None


@dataclass
class TempFindParagraph:
    rid: str
    field: str
    score: float
    start: int
    end: int
    id: str
    paragraph: Optional[FindParagraph] = None
    vector_index: Optional[DocumentScored] = None
    paragraph_index: Optional[PBParagraphResult] = None


class FindField(BaseModel):
    paragraphs: Dict[str, FindParagraph]


class FindResource(Resource):
    fields: Dict[str, FindField]

    def updated_from(self, origin: Resource):
        for key in origin.__fields__.keys():
            self.__setattr__(key, getattr(origin, key))


class KnowledgeboxFindResults(BaseModel):
    resources: Dict[str, FindResource]
    relations: Optional[Relations] = None
    facets: FacetsResult
    query: Optional[str] = None
    total: int = 0
    page_number: int = 0
    page_size: int = 20
    next_page: bool = False
    nodes: Optional[List[Tuple[str, str, str]]]
    shards: Optional[List[str]]


class FeedbackTasks(str, Enum):
    CHAT = "CHAT"


class FeedbackRequest(BaseModel):
    ident: str
    good: bool
    task: FeedbackTasks
    feedback: Optional[str]
