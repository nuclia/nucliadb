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
from typing import Any, Dict, List, Literal, Optional, Set, Type, TypeVar, Union

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, Field, field_validator, model_validator
from typing_extensions import Annotated, Self

from nucliadb_models.common import FieldTypeName, ParamDefault
from nucliadb_models.metadata import RelationType, ResourceProcessingStatus
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.security import RequestSecurity
from nucliadb_models.vectors import SemanticModelMetadata, VectorSimilarity
from nucliadb_protos.audit_pb2 import ClientType
from nucliadb_protos.nodereader_pb2 import DocumentScored, OrderBy
from nucliadb_protos.nodereader_pb2 import ParagraphResult as PBParagraphResult
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2 import ShardObject as PBShardObject
from nucliadb_protos.writer_pb2 import Shards as PBShards

_T = TypeVar("_T")

ANSWER_JSON_SCHEMA_EXAMPLE = {
    "name": "structred_response",
    "description": "Structured response with custom fields",
    "parameters": {
        "type": "object",
        "properties": {
            "answer": {
                "type": "string",
                "description": "Text responding to the user's query with the given context.",
            },
            "confidence": {
                "type": "integer",
                "description": "The confidence level of the response, on a scale from 0 to 5.",
                "minimum": 0,
                "maximum": 5,
            },
            "machinery_mentioned": {
                "type": "array",
                "items": {
                    "type": "string",
                    "description": "A list of machinery mentioned in the response, if any. Use machine IDs if possible.",
                },
                "description": "Optional field listing any machinery mentioned in the response.",
            },
        },
        "required": ["answer", "confidence"],
    },
}


class ModelParamDefaults:
    applied_autofilters = ParamDefault(
        default=[],
        title="Autofilters",
        description="List of filters automatically applied to the search query",
    )


class ResourceProperties(str, Enum):
    BASIC = "basic"
    ORIGIN = "origin"
    EXTRA = "extra"
    RELATIONS = "relations"
    VALUES = "values"
    EXTRACTED = "extracted"
    ERRORS = "errors"
    SECURITY = "security"


class SearchOptions(str, Enum):
    PARAGRAPH = "paragraph"
    DOCUMENT = "document"
    RELATIONS = "relations"
    VECTOR = "vector"


class ChatOptions(str, Enum):
    VECTORS = "vectors"
    PARAGRAPHS = "paragraphs"
    RELATIONS = "relations"


class SuggestOptions(str, Enum):
    PARAGRAPH = "paragraph"
    ENTITIES = "entities"


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


class JsonBaseModel(BaseModel):
    def __str__(self):
        try:
            return self.json()
        except Exception:
            # fallback to BaseModel implementation
            return super().__str__()


class Facet(BaseModel):
    facetresults: Dict[str, int]


FacetsResult = Dict[str, Any]


class TextPosition(BaseModel):
    page_number: Optional[int] = None
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
    min_score: float = Field(
        title="Minimum score",
        description="Minimum similarity score used to filter vector index search. Results with a lower score have been ignored.",  # noqa
    )


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
    fuzzy_result: bool = False


class Paragraphs(BaseModel):
    results: List[Paragraph] = []
    facets: Optional[FacetsResult] = None
    query: Optional[str] = None
    total: int = 0
    page_number: int = 0
    page_size: int = 20
    next_page: bool = False
    min_score: float = Field(
        title="Minimum score",
        description="Minimum bm25 score used to filter bm25 index search. Results with a lower score have been ignored.",  # noqa
    )


class ResourceResult(BaseModel):
    score: Union[float, int]
    rid: str
    field_type: str
    field: str
    labels: Optional[list[str]] = None


class Resources(BaseModel):
    results: List[ResourceResult]
    facets: Optional[FacetsResult] = None
    query: Optional[str] = None
    total: int = 0
    page_number: int = 0
    page_size: int = 20
    next_page: bool = False
    min_score: float = Field(
        title="Minimum score",
        description="Minimum bm25 score used to filter bm25 index search. Results with a lower score have been ignored.",  # noqa
    )


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


class SentenceSearch(BaseModel):
    data: List[float] = []
    time: float


class Ner(BaseModel):
    text: str
    ner: str
    start: int
    end: int


class TokenSearch(BaseModel):
    tokens: List[Ner] = []
    time: float


class QueryInfo(BaseModel):
    language: Optional[str] = None
    stop_words: List[str] = []
    semantic_threshold: Optional[float] = None
    visual_llm: bool
    max_context: int
    entities: TokenSearch
    sentence: SentenceSearch
    query: str


class Relations(BaseModel):
    entities: Dict[str, EntitySubgraph]
    # TODO: implement in the next iteration of knowledge graph search
    # graph: List[RelationPath]


class RelatedEntity(BaseModel, frozen=True):
    family: str
    value: str


class RelatedEntities(BaseModel):
    total: int = 0
    entities: List[RelatedEntity] = []


class ResourceSearchResults(JsonBaseModel):
    """Search on resource results"""

    sentences: Optional[Sentences] = None
    paragraphs: Optional[Paragraphs] = None
    relations: Optional[Relations] = None
    nodes: Optional[List[Dict[str, str]]] = None
    shards: Optional[List[str]] = None


class KnowledgeboxSearchResults(JsonBaseModel):
    """Search on knowledgebox results"""

    resources: Dict[str, Resource] = {}
    sentences: Optional[Sentences] = None
    paragraphs: Optional[Paragraphs] = None
    fulltext: Optional[Resources] = None
    relations: Optional[Relations] = None
    nodes: Optional[List[Dict[str, str]]] = None
    shards: Optional[List[str]] = None
    autofilters: List[str] = ModelParamDefaults.applied_autofilters.to_pydantic_field()


class KnowledgeboxSuggestResults(JsonBaseModel):
    """Suggest on resource results"""

    paragraphs: Optional[Paragraphs] = None
    entities: Optional[RelatedEntities] = None
    shards: Optional[List[str]] = None


class KnowledgeboxCounters(BaseModel):
    resources: int
    paragraphs: int
    fields: int
    sentences: int
    shards: Optional[List[str]] = None
    index_size: float = Field(default=0.0, title="Index size (bytes)")


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
    DOCUMENT_V2 = "DOCUMENT_V2"


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
    model: Optional[SemanticModelMetadata] = None

    @classmethod
    def from_message(cls: Type[_T], message: PBShards) -> _T:
        as_dict = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        as_dict["similarity"] = VectorSimilarity.from_message(message.similarity)
        if message.HasField("model"):
            as_dict["model"] = SemanticModelMetadata.from_message(message.model)
        return cls(**as_dict)


class SearchParamDefaults:
    query = ParamDefault(default="", title="Query", description="The query to search for")
    suggest_query = ParamDefault(
        default=..., title="Query", description="The query to get suggestions for"
    )
    fields = ParamDefault(
        default=[],
        title="Fields",
        description="The list of fields to search in. For instance: `a/title` to search only on title field. For more details on filtering by field, see: https://docs.nuclia.dev/docs/docs/using/search/#search-in-a-specific-field",  # noqa: E501
    )
    filters = ParamDefault(
        default=[],
        title="Filters",
        description="The list of filters to apply. Filtering examples can be found here: https://docs.nuclia.dev/docs/docs/using/search/#filters",  # noqa: E501
    )
    resource_filters = ParamDefault(
        default=[],
        title="Resources filter",
        description="List of resource ids to filter search results for. Only paragraphs from the specified resources will be returned.",  # noqa: E501
    )
    faceted = ParamDefault(
        default=[],
        title="Faceted",
        description="The list of facets to calculate. The facets follow the same syntax as filters: https://docs.nuclia.dev/docs/docs/using/search/#filters",  # noqa: E501
        max_items=50,
    )
    autofilter = ParamDefault(
        default=False,
        title="Automatic search filtering",
        description="If set to true, the search will automatically add filters to the query. For example, it will filter results containing the entities detected in the query",  # noqa: E501
    )
    chat_query = ParamDefault(
        default=...,
        title="Query",
        description="The query to get a generative answer for",
    )
    shards = ParamDefault(
        default=[],
        title="Shards",
        description="The list of shard replicas to search in. If empty, random replicas will be selected.",
    )
    page_number = ParamDefault(
        default=0,
        title="Page number",
        description="The page number of the results to return",
    )
    page_size = ParamDefault(
        default=20,
        le=200,
        title="Page size",
        description="The number of results to return per page. The maximum number of results per page allowed is 200.",
    )
    highlight = ParamDefault(
        default=False,
        title="Highlight",
        description="If set to true, the query terms will be highlighted in the results between <mark>...</mark> tags",  # noqa: E501
    )
    with_duplicates = ParamDefault(
        default=False,
        title="With duplicate paragraphs",
        description="Whether to return duplicate paragraphs on the same document",  # noqa: E501
    )
    with_status = ParamDefault(
        default=None,
        title="With processing status",
        description="Filter results by resource processing status",
    )
    with_synonyms = ParamDefault(
        default=False,
        title="With custom synonyms",
        description="Whether to return matches for custom knowledge box synonyms of the query terms. Note: only supported for `paragraph` and `document` search options.",  # noqa: E501
    )
    sort_order = ParamDefault(
        default=SortOrder.DESC,
        title="Sort order",
        description="Order to sort results with",
    )
    sort_limit = ParamDefault(
        default=None,
        title="Sort limit",
        description="",
        gt=0,
    )
    sort_field = ParamDefault(
        default=None,
        title="Sort field",
        description="Field to sort results with",
    )
    sort = ParamDefault(
        default=None,
        title="Sort options",
        description="Options for results sorting",
    )
    search_features = ParamDefault(
        default=None,
        title="Search features",
        description="List of search features to use. Each value corresponds to a lookup into on of the different indexes.",  # noqa
    )
    debug = ParamDefault(
        default=False,
        title="Debug mode",
        description="If set, the response will include some extra metadata for debugging purposes, like the list of queried nodes.",  # noqa
    )
    show = ParamDefault(
        default=[ResourceProperties.BASIC],
        title="Show metadata",
        description="Controls which types of metadata are serialized on resources of search results",
    )
    extracted = ParamDefault(
        default=[],
        title="Extracted metadata",
        description="Controls which parts of the extracted metadata are serialized on search results",
    )
    field_type_filter = ParamDefault(
        default=list(FieldTypeName),
        title="Field type filter",
        description="Filter search results to match paragraphs of a specific field type. E.g: `['conversation', 'text']`",  # noqa
    )
    range_creation_start = ParamDefault(
        default=None,
        title="Resource creation range start",
        description="Resources created before this date will be filtered out of search results. Datetime are represented as a str in ISO 8601 format, like: 2008-09-15T15:53:00+05:00.",  # noqa
    )
    range_creation_end = ParamDefault(
        default=None,
        title="Resource creation range end",
        description="Resources created after this date will be filtered out of search results. Datetime are represented as a str in ISO 8601 format, like: 2008-09-15T15:53:00+05:00.",  # noqa
    )
    range_modification_start = ParamDefault(
        default=None,
        title="Resource modification range start",
        description="Resources modified before this date will be filtered out of search results. Datetime are represented as a str in ISO 8601 format, like: 2008-09-15T15:53:00+05:00.",  # noqa
    )
    range_modification_end = ParamDefault(
        default=None,
        title="Resource modification range end",
        description="Resources modified after this date will be filtered out of search results. Datetime are represented as a str in ISO 8601 format, like: 2008-09-15T15:53:00+05:00.",  # noqa
    )
    vector = ParamDefault(
        default=None,
        title="Search Vector",
        description="The vector to perform the search with. If not provided, NucliaDB will use Nuclia Predict API to create the vector off from the query.",  # noqa
    )
    vectorset = ParamDefault(
        default=None,
        title="Vectorset",
        description="Vectors index to perform the search in. If not provided, NucliaDB will use the default one",
    )
    chat_context = ParamDefault(
        default=None,
        title="Chat history",
        description="Use to rephrase the new LLM query by taking into account the chat conversation history",  # noqa
    )
    chat_features = ParamDefault(
        default=[ChatOptions.VECTORS, ChatOptions.PARAGRAPHS, ChatOptions.RELATIONS],
        title="Chat features",
        description="Features enabled for the chat endpoint. Semantic search is done if `vectors` is included. If `paragraphs` is included, the results will include matching paragraphs from the bm25 index. If `relations` is included, a graph of entities related to the answer is returned.",  # noqa
    )
    suggest_features = ParamDefault(
        default=[
            SuggestOptions.PARAGRAPH,
            SuggestOptions.ENTITIES,
        ],
        title="Suggest features",
        description="Features enabled for the suggest endpoint.",
    )
    security = ParamDefault(
        default=None,
        title="Security",
        description="Security metadata for the request. If not provided, the search request is done without the security lookup phase.",  # noqa
    )
    security_groups = ParamDefault(
        default=[],
        title="Security groups",
        description="List of security groups to filter search results for. Only resources matching the query and containing the specified security groups will be returned. If empty, all resources will be considered for the search.",  # noqa
    )
    rephrase = ParamDefault(
        default=False,
        title="Rephrase query consuming LLMs",
        description="Rephrase query consuming LLMs - it will make the query slower",  # noqa
    )
    prefer_markdown = ParamDefault(
        default=False,
        title="Prefer markdown",
        description="If set to true, the response will be in markdown format",
    )


class Filter(BaseModel):
    all: Optional[List[str]] = Field(default=None, min_length=1)
    any: Optional[List[str]] = Field(default=None, min_length=1)
    none: Optional[List[str]] = Field(default=None, min_length=1)
    not_all: Optional[List[str]] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_filter(self) -> Self:
        if (self.all, self.any, self.none, self.not_all).count(None) != 3:
            raise ValueError("Only one of 'all', 'any', 'none' or 'not_all' can be set")
        return self


class CatalogRequest(BaseModel):
    query: str = SearchParamDefaults.query.to_pydantic_field()
    filters: Union[List[str], List[Filter]] = Field(
        default=[],
        title="Filters",
        description="The list of filters to apply. Filtering examples can be found here: https://docs.nuclia.dev/docs/docs/using/search/#filters",  # noqa: E501
    )
    faceted: List[str] = SearchParamDefaults.faceted.to_pydantic_field()
    sort: Optional[SortOptions] = SearchParamDefaults.sort.to_pydantic_field()
    page_number: int = SearchParamDefaults.page_number.to_pydantic_field()
    page_size: int = SearchParamDefaults.page_size.to_pydantic_field()
    shards: List[str] = SearchParamDefaults.shards.to_pydantic_field()
    debug: bool = SearchParamDefaults.debug.to_pydantic_field()
    with_status: Optional[ResourceProcessingStatus] = Field(
        default=None,
        title="With processing status",
        description="Filter results by resource processing status",
    )
    range_creation_start: Optional[datetime] = (
        SearchParamDefaults.range_creation_start.to_pydantic_field()
    )
    range_creation_end: Optional[datetime] = SearchParamDefaults.range_creation_end.to_pydantic_field()
    range_modification_start: Optional[datetime] = (
        SearchParamDefaults.range_modification_start.to_pydantic_field()
    )
    range_modification_end: Optional[datetime] = (
        SearchParamDefaults.range_modification_end.to_pydantic_field()
    )

    @field_validator("faceted")
    @classmethod
    def nested_facets_not_supported(cls, facets):
        return validate_facets(facets)


class MinScore(BaseModel):
    semantic: Optional[float] = Field(
        default=None,
        title="Minimum semantic score",
        description="Minimum semantic similarity score used to filter vector index search. If not specified, the default minimum score of the semantic model associated to the Knowledge Box will be used. Check out the documentation for more information on how to use this parameter: https://docs.nuclia.dev/docs/docs/using/search/#minimum-score",  # noqa: E501
    )
    bm25: float = Field(
        default=0,
        title="Minimum bm25 score",
        description="Minimum score used to filter bm25 index search. Check out the documentation for more information on how to use this parameter: https://docs.nuclia.dev/docs/docs/using/search/#minimum-score",  # noqa: E501
        ge=0,
    )


class BaseSearchRequest(BaseModel):
    query: str = SearchParamDefaults.query.to_pydantic_field()
    fields: List[str] = SearchParamDefaults.fields.to_pydantic_field()
    filters: Union[List[str], List[Filter]] = Field(
        default=[],
        title="Filters",
        description="The list of filters to apply. Filtering examples can be found here: https://docs.nuclia.dev/docs/docs/using/search/#filters",  # noqa: E501
    )
    page_number: int = SearchParamDefaults.page_number.to_pydantic_field()
    page_size: int = SearchParamDefaults.page_size.to_pydantic_field()
    min_score: Optional[Union[float, MinScore]] = Field(
        default=None,
        title="Minimum score",
        description="Minimum score to filter search results. Results with a lower score will be ignored. Accepts either a float or a dictionary with the minimum scores for the bm25 and vector indexes. If a float is provided, it is interpreted as the minimum score for vector index search.",  # noqa
    )
    range_creation_start: Optional[datetime] = (
        SearchParamDefaults.range_creation_start.to_pydantic_field()
    )
    range_creation_end: Optional[datetime] = SearchParamDefaults.range_creation_end.to_pydantic_field()
    range_modification_start: Optional[datetime] = (
        SearchParamDefaults.range_modification_start.to_pydantic_field()
    )
    range_modification_end: Optional[datetime] = (
        SearchParamDefaults.range_modification_end.to_pydantic_field()
    )
    features: List[SearchOptions] = SearchParamDefaults.search_features.to_pydantic_field(
        default=[
            SearchOptions.PARAGRAPH,
            SearchOptions.DOCUMENT,
            SearchOptions.VECTOR,
        ]
    )
    debug: bool = SearchParamDefaults.debug.to_pydantic_field()
    highlight: bool = SearchParamDefaults.highlight.to_pydantic_field()
    show: List[ResourceProperties] = SearchParamDefaults.show.to_pydantic_field()
    field_type_filter: List[FieldTypeName] = SearchParamDefaults.field_type_filter.to_pydantic_field()
    extracted: List[ExtractedDataTypeName] = SearchParamDefaults.extracted.to_pydantic_field()
    shards: List[str] = SearchParamDefaults.shards.to_pydantic_field()
    vector: Optional[List[float]] = SearchParamDefaults.vector.to_pydantic_field()
    vectorset: Optional[str] = SearchParamDefaults.vectorset.to_pydantic_field()
    with_duplicates: bool = SearchParamDefaults.with_duplicates.to_pydantic_field()
    with_synonyms: bool = SearchParamDefaults.with_synonyms.to_pydantic_field()
    autofilter: bool = SearchParamDefaults.autofilter.to_pydantic_field()
    resource_filters: List[str] = SearchParamDefaults.resource_filters.to_pydantic_field()
    security: Optional[RequestSecurity] = SearchParamDefaults.security.to_pydantic_field()

    rephrase: bool = Field(
        default=False,
        title="Rephrase the query to improve search",
        description="Consume LLM tokens to rephrase the query so the semantic search is better",
    )


class SearchRequest(BaseSearchRequest):
    faceted: List[str] = SearchParamDefaults.faceted.to_pydantic_field()
    sort: Optional[SortOptions] = SearchParamDefaults.sort.to_pydantic_field()

    @field_validator("faceted")
    @classmethod
    def nested_facets_not_supported(cls, facets):
        return validate_facets(facets)


class Author(str, Enum):
    NUCLIA = "NUCLIA"
    USER = "USER"


class ChatContextMessage(BaseModel):
    author: Author
    text: str


# For bw compatibility
Message = ChatContextMessage


class UserPrompt(BaseModel):
    prompt: str


class Image(BaseModel):
    content_type: str
    b64encoded: str


class MaxTokens(BaseModel):
    context: Optional[int] = Field(
        default=None,
        title="Maximum context tokens",
        description="Use to limit the amount of tokens used in the LLM context",
    )
    answer: Optional[int] = Field(
        default=None,
        title="Maximum answer tokens",
        description="Use to limit the amount of tokens used in the LLM answer",
    )


def parse_max_tokens(max_tokens: Optional[Union[int, MaxTokens]]) -> Optional[MaxTokens]:
    if isinstance(max_tokens, int):
        # If the max_tokens is an integer, it is interpreted as the max_tokens value for the generated answer.
        # The max tokens for the context is set to None to use the default value for the model (comes in the
        # NUA's query endpoint response).
        return MaxTokens(answer=max_tokens, context=None)
    return max_tokens


class ChatModel(BaseModel):
    """
    This is the model for the predict request payload on the chat endpoint
    """

    question: str = Field(description="Question to ask the generative model")
    user_id: str
    retrieval: bool = True
    system: Optional[str] = Field(
        default=None,
        title="System prompt",
        description="Optional system prompt input by the user",
    )
    query_context: Dict[str, str] = Field(
        default={},
        description="The information retrieval context for the current query",
    )
    query_context_order: Optional[Dict[str, int]] = Field(
        default=None,
        description="The order of the query context elements. This is used to sort the context elements by relevance before sending them to the generative model",  # noqa
    )
    chat_history: List[ChatContextMessage] = Field(
        default=[], description="The chat conversation history"
    )
    truncate: bool = Field(
        default=True,
        description="Truncate the chat context in case it doesn't fit the generative input",
    )
    user_prompt: Optional[UserPrompt] = Field(
        default=None, description="Optional custom prompt input by the user"
    )
    citations: bool = Field(default=False, description="Whether to include the citations in the answer")
    generative_model: Optional[str] = Field(
        default=None,
        title="Generative model",
        description="The generative model to use for the predict chat endpoint. If not provided, the model configured for the Knowledge Box is used.",  # noqa: E501
    )

    max_tokens: Optional[int] = Field(default=None, description="Maximum characters to generate")

    query_context_images: Dict[str, Image] = Field(
        default={},
        description="The information retrieval context for the current query, each image is a base64 encoded string",
    )

    prefer_markdown: bool = Field(
        default=False,
        description="If set to true, the response will be in markdown format",
    )
    json_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description="The JSON schema to use for the generative model answers",
    )


class RephraseModel(BaseModel):
    question: str
    chat_history: List[ChatContextMessage] = []
    user_id: str
    user_context: List[str] = []
    generative_model: Optional[str] = Field(
        default=None,
        title="Generative model",
        description="The generative model to use for the rephrase endpoint. If not provided, the model configured for the Knowledge Box is used.",  # noqa: E501
    )


class RagStrategyName:
    FIELD_EXTENSION = "field_extension"
    FULL_RESOURCE = "full_resource"
    HIERARCHY = "hierarchy"


class ImageRagStrategyName:
    PAGE_IMAGE = "page_image"
    TABLES = "tables"


class RagStrategy(BaseModel):
    name: str


class ImageRagStrategy(BaseModel):
    name: str


ALLOWED_FIELD_TYPES: dict[str, str] = {
    "t": "text",
    "f": "file",
    "u": "link",
    "d": "datetime",
    "a": "generic",
}


class FieldExtensionStrategy(RagStrategy):
    name: Literal["field_extension"]
    fields: Set[str] = Field(
        title="Fields",
        description="List of field ids to extend the context with. It will try to extend the retrieval context with the specified fields in the matching resources. The field ids have to be in the format `{field_type}/{field_name}`, like 'a/title', 'a/summary' for title and summary fields or 't/amend' for a text field named 'amend'.",  # noqa
        min_length=1,
    )

    @field_validator("fields", mode="after")
    @classmethod
    def fields_validator(cls, fields) -> Self:
        # Check that the fields are in the format {field_type}/{field_name}
        for field in fields:
            try:
                field_type, _ = field.strip("/").split("/")
            except ValueError:
                raise ValueError(f"Field '{field}' is not in the format {{field_type}}/{{field_name}}")
            if field_type not in ALLOWED_FIELD_TYPES:
                allowed_field_types_part = ", ".join(
                    [f"'{fid}' for '{fname}' fields" for fid, fname in ALLOWED_FIELD_TYPES.items()]
                )
                raise ValueError(
                    f"Field '{field}' does not have a valid field type. "
                    f"Valid field types are: {allowed_field_types_part}."
                )

        return fields


class FullResourceStrategy(RagStrategy):
    name: Literal["full_resource"]
    count: Optional[int] = Field(
        default=None,
        title="Count",
        description="Maximum number of full documents to retrieve. If not specified, all matching documents are retrieved.",
    )


class HierarchyResourceStrategy(RagStrategy):
    name: Literal["hierarchy"]
    count: Optional[int] = Field(
        default=None,
        title="Count",
        description="Number of extra characters that are added to each matching paragraph when adding to the context.",
    )


class TableImageStrategy(ImageRagStrategy):
    name: Literal["tables"]


class PageImageStrategy(ImageRagStrategy):
    name: Literal["page_image"]
    count: Optional[int] = Field(
        default=None,
        title="Count",
        description="Maximum number of images to retrieve from the page. By default, at most 5 images are retrieved.",
    )


class ParagraphImageStrategy(ImageRagStrategy):
    name: Literal["paragraph_image"]


RagStrategies = Annotated[
    Union[FieldExtensionStrategy, FullResourceStrategy, HierarchyResourceStrategy],
    Field(discriminator="name"),
]
RagImagesStrategies = Annotated[
    Union[PageImageStrategy, ParagraphImageStrategy], Field(discriminator="name")
]
PromptContext = dict[str, str]
PromptContextOrder = dict[str, int]
PromptContextImages = dict[str, Image]


class CustomPrompt(BaseModel):
    system: Optional[str] = Field(
        default=None,
        title="System prompt",
        description="System prompt given to the generative model. This can help customize the behavior of the model. If not specified, the default model provider's prompt is used.",  # noqa
        min_length=1,
        examples=[
            "You are a medical assistant, use medical terminology",
            "You are an IT expert, express yourself like one",
            "You are a very friendly customer service assistant, be polite",
            "You are a financial expert, use correct terms",
        ],
    )
    user: Optional[str] = Field(
        default=None,
        title="User prompt",
        description="User prompt given to the generative model. Use the words {context} and {question} in brackets where you want those fields to be placed, in case you want them in your prompt. Context will be the data returned by the retrieval step and question will be the user's query.",  # noqa
        min_length=1,
        examples=[
            "Taking into account our previous conversation, and this context: {context} answer this {question}",
            "Give a detailed answer to this {question} in a list format. If you do not find an answer in this context: {context}, say that you don't have enough data.",
            "Given this context: {context}. Answer this {question} in a concise way using the provided context",
            "Given this context: {context}. Answer this {question} using the provided context. Please, answer always in French",
        ],
    )


class ChatRequest(BaseModel):
    query: str = SearchParamDefaults.chat_query.to_pydantic_field()
    fields: List[str] = SearchParamDefaults.fields.to_pydantic_field()
    filters: Union[List[str], List[Filter]] = Field(
        default=[],
        title="Filters",
        description="The list of filters to apply. Filtering examples can be found here: https://docs.nuclia.dev/docs/docs/using/search/#filters",  # noqa: E501
    )
    vectorset: Optional[str] = SearchParamDefaults.vectorset.to_pydantic_field()
    min_score: Optional[Union[float, MinScore]] = Field(
        default=None,
        title="Minimum score",
        description="Minimum score to filter search results. Results with a lower score will be ignored. Accepts either a float or a dictionary with the minimum scores for the bm25 and vector indexes. If a float is provided, it is interpreted as the minimum score for vector index search.",  # noqa
    )
    features: List[ChatOptions] = SearchParamDefaults.chat_features.to_pydantic_field()
    range_creation_start: Optional[datetime] = (
        SearchParamDefaults.range_creation_start.to_pydantic_field()
    )
    range_creation_end: Optional[datetime] = SearchParamDefaults.range_creation_end.to_pydantic_field()
    range_modification_start: Optional[datetime] = (
        SearchParamDefaults.range_modification_start.to_pydantic_field()
    )
    range_modification_end: Optional[datetime] = (
        SearchParamDefaults.range_modification_end.to_pydantic_field()
    )
    show: List[ResourceProperties] = SearchParamDefaults.show.to_pydantic_field()
    field_type_filter: List[FieldTypeName] = SearchParamDefaults.field_type_filter.to_pydantic_field()
    extracted: List[ExtractedDataTypeName] = SearchParamDefaults.extracted.to_pydantic_field()
    shards: List[str] = SearchParamDefaults.shards.to_pydantic_field()
    context: Optional[List[ChatContextMessage]] = SearchParamDefaults.chat_context.to_pydantic_field()
    extra_context: Optional[List[str]] = Field(
        default=None,
        title="Extra query context",
        description="""Additional context that is added to the retrieval context sent to the LLM.
        It allows extending the chat feature with content that may not be in the Knowledge Box.""",
    )
    autofilter: bool = SearchParamDefaults.autofilter.to_pydantic_field()
    highlight: bool = SearchParamDefaults.highlight.to_pydantic_field()
    resource_filters: List[str] = SearchParamDefaults.resource_filters.to_pydantic_field()
    prompt: Optional[Union[str, CustomPrompt]] = Field(
        default=None,
        title="Prompts",
        description="Use to customize the prompts given to the generative model. Both system and user prompts can be customized. If a string is provided, it is interpreted as the user prompt.",  # noqa
    )
    citations: bool = Field(
        default=False,
        description="Whether to include the citations for the answer in the response",
    )
    security: Optional[RequestSecurity] = SearchParamDefaults.security.to_pydantic_field()
    rag_strategies: list[RagStrategies] = Field(
        default=[],
        title="RAG context building strategies",
        description="Options for tweaking how the context for the LLM model is crafted. `full_resource` will add the full text of the matching resources to the context. `field_extension` will add the text of the matching resource's specified fields to the context. If empty, the default strategy is used.",  # noqa
    )
    rag_images_strategies: list[RagImagesStrategies] = Field(
        default=[],
        title="RAG image context building strategies",
        description="Options for tweaking how the image based context for the LLM model is crafted. `page_image` will add the full page image of the matching resources to the context. If empty, the default strategy is used with the image of the paragraph.",  # noqa
    )
    debug: bool = SearchParamDefaults.debug.to_pydantic_field()

    generative_model: Optional[str] = Field(
        default=None,
        title="Generative model",
        description="The generative model to use for the chat endpoint. If not provided, the model configured for the Knowledge Box is used.",  # noqa: E501
    )

    max_tokens: Optional[Union[int, MaxTokens]] = Field(
        default=None,
        title="Maximum LLM tokens to use for the request",
        description="Use to limit the amount of tokens used in the LLM context and/or for generating the answer. If not provided, the default maximum tokens of the generative model will be used. If an integer is provided, it is interpreted as the maximum tokens for the answer.",  # noqa
    )

    rephrase: bool = Field(
        default=False,
        title="Rephrase the query to improve search",
        description="Consume LLM tokens to rephrase the query so the semantic search is better",
    )

    prefer_markdown: bool = Field(
        default=False,
        title="Prefer markdown",
        description="If set to true, the response will be in markdown format",
    )

    @field_validator("rag_strategies", mode="before")
    @classmethod
    def validate_rag_strategies(cls, rag_strategies: list[RagStrategies]) -> list[RagStrategies]:
        unique_strategy_names: set[str] = set()
        for strategy in rag_strategies or []:
            if not isinstance(strategy, dict):
                raise ValueError("RAG strategies must be defined using an object")
            strategy_name = strategy.get("name")
            if strategy_name is None:
                raise ValueError(f"Invalid strategy '{strategy}'")
            unique_strategy_names.add(strategy_name)
        if len(unique_strategy_names) != len(rag_strategies):
            raise ValueError("There must be at most one strategy of each type")

        # If full_resource or hierarchy strategies is chosen, they must be the only strategy
        for unique_strategy_name in (RagStrategyName.FULL_RESOURCE, RagStrategyName.HIERARCHY):
            if unique_strategy_name in unique_strategy_names and len(rag_strategies) > 1:
                raise ValueError(
                    f"If '{unique_strategy_name}' strategy is chosen, it must be the only strategy."
                )
        return rag_strategies


class SummarizeResourceModel(BaseModel):
    fields: Dict[str, str] = {}


class SummaryKind(str, Enum):
    SIMPLE = "simple"
    EXTENDED = "extended"


class SummarizeModel(BaseModel):
    """
    Model for the summarize predict api request payload
    """

    resources: Dict[str, SummarizeResourceModel] = {}
    generative_model: Optional[str] = None
    user_prompt: Optional[str] = None
    summary_kind: SummaryKind = SummaryKind.SIMPLE


class SummarizeRequest(BaseModel):
    """
    Model for the request payload of the summarize endpoint
    """

    generative_model: Optional[str] = Field(
        default=None,
        title="Generative model",
        description="The generative model to use for the summarization. If not provided, the model configured for the Knowledge Box is used.",  # noqa: E501
    )

    user_prompt: Optional[str] = Field(
        default=None,
        title="User prompt",
        description="Optional custom prompt input by the user",
    )

    resources: List[str] = Field(
        ...,
        min_length=1,
        max_length=100,
        title="Resources",
        description="Uids or slugs of the resources to summarize. If the resources are not found, they will be ignored.",  # noqa: E501
    )

    summary_kind: SummaryKind = Field(
        default=SummaryKind.SIMPLE,
        title="Summary kind",
        description="Option to customize how the summary will be",
    )


class SummarizedResource(BaseModel):
    summary: str = Field(..., title="Summary", description="Summary of the resource")
    tokens: int


class SummarizedResponse(BaseModel):
    resources: Dict[str, SummarizedResource] = Field(
        default={},
        title="Resources",
        description="Individual resource summaries. The key is the resource id or slug.",
    )
    summary: str = Field(
        default="",
        title="Summary",
        description="Global summary of all resources combined.",
    )


class FindRequest(BaseSearchRequest):
    features: List[SearchOptions] = SearchParamDefaults.search_features.to_pydantic_field(
        default=[
            SearchOptions.PARAGRAPH,
            SearchOptions.VECTOR,
        ]
    )

    @field_validator("features")
    @classmethod
    def fulltext_not_supported(cls, v):
        if SearchOptions.DOCUMENT in v or SearchOptions.DOCUMENT == v:
            raise ValueError("fulltext search not supported")
        return v


class SCORE_TYPE(str, Enum):
    VECTOR = "VECTOR"
    BM25 = "BM25"
    BOTH = "BOTH"


class FindTextPosition(BaseModel):
    page_number: Optional[int] = None
    start_seconds: Optional[List[int]] = None
    end_seconds: Optional[List[int]] = None
    index: int
    start: int
    end: int


class FindParagraph(BaseModel):
    score: float
    score_type: SCORE_TYPE
    order: int = Field(default=0, ge=0)
    text: str
    id: str
    labels: Optional[List[str]] = []
    position: Optional[TextPosition] = None
    fuzzy_result: bool = False
    page_with_visual: bool = Field(
        default=False,
        title="Page where this paragraph belongs is a visual page",
        description="This flag informs if the page may have information that has not been extracted",
    )
    reference: Optional[str] = Field(
        default=None,
        title="Reference to the image that represents this text",
        description="Reference to the extracted image that represents this paragraph",
    )
    is_a_table: bool = Field(
        default=False,
        title="Is a table",
        description="The referenced image of the paragraph is a table",
    )


@dataclass
class TempFindParagraph:
    rid: str
    field: str
    score: float
    start: int
    end: int
    id: str
    split: Optional[str] = None
    paragraph: Optional[FindParagraph] = None
    vector_index: Optional[DocumentScored] = None
    paragraph_index: Optional[PBParagraphResult] = None
    fuzzy_result: bool = False
    page_with_visual: bool = False
    reference: Optional[str] = None
    is_a_table: bool = False


class FindField(BaseModel):
    paragraphs: Dict[str, FindParagraph]


class FindResource(Resource):
    fields: Dict[str, FindField]

    def updated_from(self, origin: Resource):
        for key in origin.model_fields.keys():
            self.__setattr__(key, getattr(origin, key))


class KnowledgeboxFindResults(JsonBaseModel):
    """Find on knowledgebox results"""

    resources: Dict[str, FindResource]
    relations: Optional[Relations] = None
    query: Optional[str] = None
    total: int = 0
    page_number: int = 0
    page_size: int = 20
    next_page: bool = False
    nodes: Optional[List[Dict[str, str]]] = Field(
        default=None,
        title="Nodes",
        description="List of nodes queried in the search",
    )
    shards: Optional[List[str]] = Field(
        default=None,
        title="Shards",
        description="The list of shard replica ids used for the search.",
    )
    autofilters: List[str] = ModelParamDefaults.applied_autofilters.to_pydantic_field()
    min_score: Optional[Union[float, MinScore]] = Field(
        default=MinScore(),
        title="Minimum result score",
        description="The minimum scores that have been used for the search operation.",
    )
    best_matches: List[str] = Field(
        default=[],
        title="Best matches",
        description="List of ids of best matching paragraphs. The list is sorted by decreasing relevance (most relevant first).",  # noqa
    )


class FeedbackTasks(str, Enum):
    CHAT = "CHAT"


class FeedbackRequest(BaseModel):
    ident: str = Field(
        title="Request identifier",
        description="Id of the request to provide feedback for. This id is returned in the response header `Nuclia-Learning-Id` of the chat endpoint.",  # noqa
    )
    good: bool = Field(title="Good", description="Whether the result was good or not")
    task: FeedbackTasks = Field(
        title="Task",
        description="The task the feedback is for. For now, only `CHAT` task is available",
    )
    feedback: Optional[str] = Field(None, title="Feedback", description="Feedback text")


def validate_facets(facets):
    """
    Raises ValueError if provided facets contains nested facets, like:
    ["/a/b", "/a/b/c"]
    """
    if len(facets) < 2:
        return facets

    # Sort facets alphabetically to make sure that nested facets appear right after their parents
    sorted_facets = sorted(facets)
    facet = sorted_facets.pop(0)
    while True:
        try:
            next_facet = sorted_facets.pop(0)
        except IndexError:
            # No more facets to check
            break
        if next_facet == facet:
            raise ValueError(
                f"Facet {next_facet} is already present in facets. Faceted list must be unique."
            )
        if next_facet.startswith(facet):
            if next_facet.replace(facet, "").startswith("/"):
                raise ValueError(
                    "Nested facets are not allowed: {child} is a child of {parent}".format(
                        child=next_facet, parent=facet
                    )
                )
        facet = next_facet
    return facets


class AskRequest(ChatRequest):
    answer_json_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        title="Answer JSON schema",
        description="""Desired JSON schema for the LLM answer.
This schema is passed to the LLM so that it answers in a scructured format following the schema. If not provided, textual response is returned.
Note that when using this parameter, the answer in the generative response will not be returned in chunks, the whole response text will be returned instead.
Using this feature also disables the `citations` parameter. For maximal accuracy, please include a `description` for each field of the schema.
""",
        examples=[ANSWER_JSON_SCHEMA_EXAMPLE],
    )


class AskTokens(BaseModel):
    input: int = Field(
        title="Input tokens",
        description="Number of LLM tokens used for the context in the query",
    )
    output: int = Field(
        title="Output tokens",
        description="Number of LLM tokens used for the answer",
    )


class AskTimings(BaseModel):
    generative_first_chunk: Optional[float] = Field(
        title="Generative first chunk",
        description="Time the LLM took to generate the first chunk of the answer",
    )
    generative_total: Optional[float] = Field(
        title="Generative total",
        description="Total time the LLM took to generate the answer",
    )


class SyncAskMetadata(BaseModel):
    tokens: Optional[AskTokens] = Field(
        default=None,
        title="Tokens",
        description="Number of tokens used in the LLM context and answer",
    )
    timings: Optional[AskTimings] = Field(
        default=None,
        title="Timings",
        description="Timings of the generative model",
    )


class SyncAskResponse(BaseModel):
    answer: str = Field(
        title="Answer",
        description="The generative answer to the query",
    )
    answer_json: Optional[Dict[str, Any]] = Field(
        default=None,
        title="Answer JSON",
        description="The generative JSON answer to the query. This is returned only if the answer_json_schema parameter is provided in the request.",  # noqa
    )
    status: str = Field(
        title="Status",
        description="The status of the query execution. It can be 'success', 'error' or 'no_context'",  # noqa
    )
    retrieval_results: KnowledgeboxFindResults = Field(
        title="Retrieval results",
        description="The retrieval results of the query",
    )
    learning_id: str = Field(
        default="",
        title="Learning id",
        description="The id of the learning request. This id can be used to provide feedback on the learning process.",  # noqa
    )
    relations: Optional[Relations] = Field(
        default=None,
        title="Relations",
        description="The detected relations of the answer",
    )
    citations: dict[str, Any] = Field(
        default={},
        title="Citations",
        description="The citations of the answer. List of references to the resources used to generate the answer.",
    )
    prompt_context: Optional[list[str]] = Field(
        default=None,
        title="Prompt context",
        description="The prompt context used to generate the answer. Returned only if the debug flag is set to true",
    )
    metadata: Optional[SyncAskMetadata] = Field(
        default=None,
        title="Metadata",
        description="Metadata of the query execution. This includes the number of tokens used in the LLM context and answer, and the timings of the generative model.",  # noqa
    )
    error_details: Optional[str] = Field(
        default=None,
        title="Error details",
        description="Error details message in case there was an error",
    )


class RetrievalAskResponseItem(BaseModel):
    type: Literal["retrieval"] = "retrieval"
    results: KnowledgeboxFindResults


class AnswerAskResponseItem(BaseModel):
    type: Literal["answer"] = "answer"
    text: str


class JSONAskResponseItem(BaseModel):
    type: Literal["answer_json"] = "answer_json"
    object: Dict[str, Any]


class MetadataAskResponseItem(BaseModel):
    type: Literal["metadata"] = "metadata"
    tokens: AskTokens
    timings: AskTimings


class CitationsAskResponseItem(BaseModel):
    type: Literal["citations"] = "citations"
    citations: dict[str, Any]


class StatusAskResponseItem(BaseModel):
    type: Literal["status"] = "status"
    code: str
    status: str
    details: Optional[str] = None


class ErrorAskResponseItem(BaseModel):
    type: Literal["error"] = "error"
    error: str


class RelationsAskResponseItem(BaseModel):
    type: Literal["relations"] = "relations"
    relations: Relations


class DebugAskResponseItem(BaseModel):
    type: Literal["debug"] = "debug"
    metadata: dict[str, Any]


AskResponseItemType = Union[
    AnswerAskResponseItem,
    JSONAskResponseItem,
    MetadataAskResponseItem,
    CitationsAskResponseItem,
    StatusAskResponseItem,
    ErrorAskResponseItem,
    RetrievalAskResponseItem,
    RelationsAskResponseItem,
    DebugAskResponseItem,
]


class AskResponseItem(BaseModel):
    item: AskResponseItemType = Field(..., discriminator="type")


def parse_custom_prompt(item: ChatRequest) -> CustomPrompt:
    prompt = CustomPrompt()
    if item.prompt is not None:
        if isinstance(item.prompt, str):
            # If the prompt is a string, it is interpreted as the user prompt
            prompt.user = item.prompt
        else:
            prompt.user = item.prompt.user
            prompt.system = item.prompt.system
    return prompt
