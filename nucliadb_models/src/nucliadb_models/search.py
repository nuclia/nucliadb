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
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, TypeVar, Union

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema
from typing_extensions import Annotated, Self

from nucliadb_models.common import FieldTypeName, ParamDefault
from nucliadb_models.metadata import RelationType, ResourceProcessingStatus
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.security import RequestSecurity
from nucliadb_models.utils import DateTime
from nucliadb_protos.audit_pb2 import ClientType, TaskType
from nucliadb_protos.nodereader_pb2 import DocumentScored, OrderBy
from nucliadb_protos.nodereader_pb2 import ParagraphResult as PBParagraphResult
from nucliadb_protos.utils_pb2 import RelationNode

# Bw/c import to avoid breaking users
from nucliadb_models.internal.predict import Ner, QueryInfo, SentenceSearch, TokenSearch  # noqa isort: skip
from nucliadb_models.internal.shards import (  # noqa isort: skip
    DocumentServiceEnum,
    ParagraphServiceEnum,
    VectorServiceEnum,
    RelationServiceEnum,
    ShardCreated,
    ShardObject,
    ShardReplica,
    KnowledgeboxShards,
)


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
    FULLTEXT = "fulltext"
    KEYWORD = "keyword"
    RELATIONS = "relations"
    SEMANTIC = "semantic"

    # DEPRECATED: use keyword, fulltext and semantic instead
    PARAGRAPH = "paragraph"
    DOCUMENT = "document"
    VECTOR = "vector"

    def normalized(self):
        if self.value == SearchOptions.PARAGRAPH:
            return SearchOptions.KEYWORD
        elif self.value == SearchOptions.DOCUMENT:
            return SearchOptions.FULLTEXT
        elif self.value == SearchOptions.VECTOR:
            return SearchOptions.SEMANTIC
        return self


class ChatOptions(str, Enum):
    KEYWORD = "keyword"
    RELATIONS = "relations"
    SEMANTIC = "semantic"

    # DEPRECATED: use keyword, and semantic instead
    VECTORS = "vectors"
    PARAGRAPHS = "paragraphs"

    def normalized(self):
        if self.value == ChatOptions.PARAGRAPHS:
            return ChatOptions.KEYWORD
        elif self.value == ChatOptions.VECTORS:
            return ChatOptions.SEMANTIC
        return self


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
            return self.model_dump_json()
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


class SearchParamDefaults:
    query = ParamDefault(default="", title="Query", description="The query to search for")
    suggest_query = ParamDefault(
        default=..., title="Query", description="The query to get suggestions for"
    )
    fields = ParamDefault(
        default=[],
        title="Fields",
        description="The list of fields to search in. For instance: `a/title` to search only on title field. For more details on filtering by field, see: https://docs.nuclia.dev/docs/rag/advanced/search/#search-in-a-specific-field",  # noqa: E501
    )
    filters = ParamDefault(
        default=[],
        title="Filters",
        description="The list of filters to apply. Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters",  # noqa: E501
    )
    resource_filters = ParamDefault(
        default=[],
        title="Resources filter",
        description="List of resource ids to filter search results for. Only paragraphs from the specified resources will be returned.",  # noqa: E501
    )
    faceted = ParamDefault(
        default=[],
        title="Faceted",
        description="The list of facets to calculate. The facets follow the same syntax as filters: https://docs.nuclia.dev/docs/rag/advanced/search/#filters",  # noqa: E501
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
    catalog_page_number = ParamDefault(
        default=0,
        title="Page number",
        description="The page number of the results to return",
    )
    catalog_page_size = ParamDefault(
        default=20,
        le=200,
        title="Page size",
        description="The number of results to return per page. The maximum number of results per page allowed is 200.",
    )
    page_number = ParamDefault(
        default=0,
        title="Page number",
        description="The page number of the results to return.\nATENTION: pagination is deprecated and this parameter will be removed soon. Please, use `top_k` instead",
        deprecated=True,
    )
    page_size = ParamDefault(
        default=20,
        le=200,
        title="Page size",
        description="The number of results to return per page. The maximum number of results per page allowed is 200.\nATENTION: pagination is deprecated and will be removed soon, pleas use to `top_k` instead",
        deprecated=True,
    )
    top_k = ParamDefault(
        default=None,
        le=200,
        title="Top k",
        description="The number of results search should return. The maximum number of results allowed is 200.",
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
        description="Field to sort results with (Score not supported in catalog)",
    )
    sort = ParamDefault(
        default=None,
        title="Sort options",
        description="Options for results sorting",
    )
    search_features = ParamDefault(
        default=None,
        title="Search features",
        description="List of search features to use. Each value corresponds to a lookup into on of the different indexes. `document`, `paragraph` and `vector` are deprecated, please use `fulltext`, `keyword` and `semantic` instead",  # noqa
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
        default=[ChatOptions.SEMANTIC, ChatOptions.KEYWORD],
        title="Chat features",
        description="Features enabled for the chat endpoint. Semantic search is done if `semantic` (or `vectors`) is included. If `keyword` (or `paragraphs`) is included, the results will include matching paragraphs from the bm25 index. If `relations` is included, a graph of entities related to the answer is returned. `paragraphs` and `vectors` are deprecated, please use `keyword` and `semantic` instead",  # noqa
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
    show_hidden = ParamDefault(
        default=False,
        title="Show hidden resources",
        description="If set to false (default), excludes hidden resources from search",
    )
    hidden = ParamDefault(
        default=None,
        title="Filter resources by hidden",
        description="Set to filter only hidden or only non-hidden resources. Default is to return everything",
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
        description="The list of filters to apply. Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters",  # noqa: E501
    )
    faceted: List[str] = SearchParamDefaults.faceted.to_pydantic_field()
    sort: Optional[SortOptions] = SearchParamDefaults.sort.to_pydantic_field()
    page_number: int = SearchParamDefaults.catalog_page_number.to_pydantic_field()
    page_size: int = SearchParamDefaults.catalog_page_size.to_pydantic_field()
    shards: List[str] = SearchParamDefaults.shards.to_pydantic_field(deprecated=True)
    debug: SkipJsonSchema[bool] = SearchParamDefaults.debug.to_pydantic_field()
    with_status: Optional[ResourceProcessingStatus] = Field(
        default=None,
        title="With processing status",
        description="Filter results by resource processing status",
        deprecated="Use filters instead",
    )
    range_creation_start: Optional[DateTime] = (
        SearchParamDefaults.range_creation_start.to_pydantic_field()
    )
    range_creation_end: Optional[DateTime] = SearchParamDefaults.range_creation_end.to_pydantic_field()
    range_modification_start: Optional[DateTime] = (
        SearchParamDefaults.range_modification_start.to_pydantic_field()
    )
    range_modification_end: Optional[DateTime] = (
        SearchParamDefaults.range_modification_end.to_pydantic_field()
    )
    hidden: SkipJsonSchema[Optional[bool]] = SearchParamDefaults.hidden.to_pydantic_field()

    @field_validator("faceted")
    @classmethod
    def nested_facets_not_supported(cls, facets):
        return validate_facets(facets)


class MinScore(BaseModel):
    semantic: Optional[float] = Field(
        default=None,
        title="Minimum semantic score",
        description="Minimum semantic similarity score used to filter vector index search. If not specified, the default minimum score of the semantic model associated to the Knowledge Box will be used. Check out the documentation for more information on how to use this parameter: https://docs.nuclia.dev/docs/rag/advanced/search#minimum-score",  # noqa: E501
    )
    bm25: float = Field(
        default=0,
        title="Minimum bm25 score",
        description="Minimum score used to filter bm25 index search. Check out the documentation for more information on how to use this parameter: https://docs.nuclia.dev/docs/rag/advanced/search#minimum-score",  # noqa: E501
        ge=0,
    )


class BaseSearchRequest(BaseModel):
    query: str = SearchParamDefaults.query.to_pydantic_field()
    fields: List[str] = SearchParamDefaults.fields.to_pydantic_field()
    filters: Union[List[str], List[Filter]] = Field(
        default=[],
        title="Filters",
        description="The list of filters to apply. Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters",  # noqa: E501
    )
    page_number: int = SearchParamDefaults.page_number.to_pydantic_field(deprecated=True)
    page_size: int = SearchParamDefaults.page_size.to_pydantic_field(deprecated=True)
    top_k: Optional[int] = SearchParamDefaults.top_k.to_pydantic_field()
    min_score: Optional[Union[float, MinScore]] = Field(
        default=None,
        title="Minimum score",
        description="Minimum score to filter search results. Results with a lower score will be ignored. Accepts either a float or a dictionary with the minimum scores for the bm25 and vector indexes. If a float is provided, it is interpreted as the minimum score for vector index search.",  # noqa
    )
    range_creation_start: Optional[DateTime] = (
        SearchParamDefaults.range_creation_start.to_pydantic_field()
    )
    range_creation_end: Optional[DateTime] = SearchParamDefaults.range_creation_end.to_pydantic_field()
    range_modification_start: Optional[DateTime] = (
        SearchParamDefaults.range_modification_start.to_pydantic_field()
    )
    range_modification_end: Optional[DateTime] = (
        SearchParamDefaults.range_modification_end.to_pydantic_field()
    )
    features: List[SearchOptions] = SearchParamDefaults.search_features.to_pydantic_field(
        default=[
            SearchOptions.KEYWORD,
            SearchOptions.FULLTEXT,
            SearchOptions.SEMANTIC,
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
    show_hidden: SkipJsonSchema[bool] = SearchParamDefaults.show_hidden.to_pydantic_field()

    rephrase: bool = Field(
        default=False,
        description=(
            "Rephrase the query for a more efficient retrieval. This will consume LLM tokens and make the request slower."
        ),
    )

    rephrase_prompt: Optional[str] = Field(
        default=None,
        title="Rephrase",
        description=(
            "Rephrase prompt given to the generative model responsible for rephrasing the query for a more effective retrieval step. "
            "This is only used if the `rephrase` flag is set to true in the request.\n"
            "If not specified, Nuclia's default prompt is used. It must include the {question} placeholder. "
            "The placeholder will be replaced with the original question"
        ),
        min_length=1,
        examples=[
            """Rephrase this question so its better for retrieval, and keep the rephrased question in the same language as the original.
QUESTION: {question}
Please return ONLY the question without any explanation. Just the rephrased question.""",
            """Rephrase this question so its better for retrieval, identify any part numbers and append them to the end of the question separated by a commas.
            QUESTION: {question}
            Please return ONLY the question without any explanation.""",
        ],
    )

    @field_validator("features", mode="after")
    @classmethod
    def normalize_features(cls, features: List[SearchOptions]):
        return [feature.normalized() for feature in features]

    @model_validator(mode="after")
    def top_k_overwrites_pagination(self):
        """This method adds support for `top_k` attribute, overwriting
        `page_number` and `page_size` if needed"""
        if self.top_k is not None:
            self.page_number = 0
            self.page_size = self.top_k
        return self


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
    citation_threshold: Optional[float] = Field(
        default=None,
        description="If citations is True, this sets the similarity threshold (0 to 1) for paragraphs to be included as citations. Lower values result in more citations. If not provided, Nuclia's default threshold is used.",  # noqa
        ge=0.0,
        le=1.0,
    )
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
    NEIGHBOURING_PARAGRAPHS = "neighbouring_paragraphs"
    METADATA_EXTENSION = "metadata_extension"
    PREQUERIES = "prequeries"


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
    name: Literal["field_extension"] = "field_extension"
    fields: list[str] = Field(
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
    name: Literal["full_resource"] = "full_resource"
    count: Optional[int] = Field(
        default=None,
        title="Count",
        description="Maximum number of full documents to retrieve. If not specified, all matching documents are retrieved.",
        ge=1,
    )
    include_remaining_text_blocks: bool = Field(
        default=False,
        title="Include remaining text blocks",
        description="Whether to include the remaining text blocks after the maximum number of resources has been reached.",
    )


class HierarchyResourceStrategy(RagStrategy):
    name: Literal["hierarchy"] = "hierarchy"
    count: int = Field(
        default=0,
        title="Count",
        description="Number of extra characters that are added to each matching paragraph when adding to the context.",
        ge=0,
    )


class NeighbouringParagraphsStrategy(RagStrategy):
    name: Literal["neighbouring_paragraphs"] = "neighbouring_paragraphs"
    before: int = Field(
        default=2,
        title="Before",
        description="Number of previous neighbouring paragraphs to add to the context, for each matching paragraph in the retrieval step.",
        ge=0,
    )
    after: int = Field(
        default=2,
        title="After",
        description="Number of following neighbouring paragraphs to add to the context, for each matching paragraph in the retrieval step.",
        ge=0,
    )


class MetadataExtensionType(str, Enum):
    ORIGIN = "origin"
    CLASSIFICATION_LABELS = "classification_labels"
    NERS = "ners"
    EXTRA_METADATA = "extra_metadata"


class MetadataExtensionStrategy(RagStrategy):
    """
    RAG strategy to enrich the context with metadata of the matching paragraphs or its resources.
    This strategy can be combined with any of the other strategies.
    """

    name: Literal["metadata_extension"] = "metadata_extension"
    types: list[MetadataExtensionType] = Field(
        min_length=1,
        title="Types",
        description="""
List of resource metadata types to add to the context.
  - 'origin': origin metadata of the resource.
  - 'classification_labels': classification labels of the resource.
  - 'ner': Named Entity Recognition entities detected for the resource.
  - 'extra_metadata': extra metadata of the resource.

Types for which the metadata is not found at the resource are ignored and not added to the context.
""",
        examples=[
            ["origin", "classification_labels"],
            ["ners"],
        ],
    )


class PreQuery(BaseModel):
    request: "FindRequest" = Field(
        title="Request",
        description="The request to be executed before the main query.",
    )
    weight: float = Field(
        default=1.0,
        title="Weight",
        description=(
            "Weight of the prequery in the context. The weight is used to scale the results of the prequery before adding them to the context."
            "The weight should be a positive number, and they are normalized so that the sum of all weights for all prequeries is 1."
        ),
        ge=0,
    )
    id: Optional[str] = Field(
        default=None,
        title="Prequery id",
        min_length=1,
        max_length=100,
        description="Identifier of the prequery. If not specified, it is autogenerated based on the index of the prequery in the list (prequery_0, prequery_1, ...).",
        examples=[
            "title_prequery",
            "summary_prequery",
            "prequery_1",
        ],
    )
    prefilter: bool = Field(
        default=False,
        title="Prefilter",
        description=(
            "If set to true, the prequery results are used to filter the scope of the remaining queries. "
            "The resources of the most relevant paragraphs of the prefilter queries are used as resource "
            "filters for the main query and other prequeries with the prefilter flag set to false."
        ),
    )


class PreQueriesStrategy(RagStrategy):
    """
    This strategy allows to run a set of queries before the main query and add the results to the context.
    It allows to give more importance to some queries over others by setting the weight of each query.
    The weight of the main query can also be set with the `main_query_weight` parameter.
    """

    name: Literal["prequeries"] = "prequeries"
    queries: list[PreQuery] = Field(
        title="Queries",
        description="List of queries to run before the main query. The results are added to the context with the specified weights for each query. There is a limit of 10 prequeries per request.",
        min_length=1,
        max_length=10,
    )
    main_query_weight: float = Field(
        default=1.0,
        title="Main query weight",
        description="Weight of the main query in the context. Use this to control the importance of the main query in the context.",
        ge=0,
    )


PreQueryResult = tuple[PreQuery, "KnowledgeboxFindResults"]


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
    Union[
        FieldExtensionStrategy,
        FullResourceStrategy,
        HierarchyResourceStrategy,
        NeighbouringParagraphsStrategy,
        MetadataExtensionStrategy,
        PreQueriesStrategy,
    ],
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
        description="System prompt given to the generative model responsible of generating the answer. This can help customize the behavior of the model when generating the answer. If not specified, the default model provider's prompt is used.",  # noqa
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
        description="User prompt given to the generative model responsible of generating the answer. Use the words {context} and {question} in brackets where you want those fields to be placed, in case you want them in your prompt. Context will be the data returned by the retrieval step and question will be the user's query.",  # noqa
        min_length=1,
        examples=[
            "Taking into account our previous conversation, and this context: {context} answer this {question}",
            "Give a detailed answer to this {question} in a list format. If you do not find an answer in this context: {context}, say that you don't have enough data.",
            "Given this context: {context}. Answer this {question} in a concise way using the provided context",
            "Given this context: {context}. Answer this {question} using the provided context. Please, answer always in French",
        ],
    )
    rephrase: Optional[str] = Field(
        default=None,
        title="Rephrase",
        description=(
            "Rephrase prompt given to the generative model responsible for rephrasing the query for a more effective retrieval step. "
            "This is only used if the `rephrase` flag is set to true in the request.\n"
            "If not specified, Nuclia's default prompt is used. It must include the {question} placeholder. "
            "The placeholder will be replaced with the original question"
        ),
        min_length=1,
        examples=[
            """Rephrase this question so its better for retrieval, and keep the rephrased question in the same language as the original.
QUESTION: {question}
Please return ONLY the question without any explanation. Just the rephrased question.""",
            """Rephrase this question so its better for retrieval, identify any part numbers and append them to the end of the question separated by a commas.
            QUESTION: {question}
            Please return ONLY the question without any explanation.""",
        ],
    )


class AskRequest(BaseModel):
    query: str = SearchParamDefaults.chat_query.to_pydantic_field()
    top_k: int = Field(
        default=20,
        title="Top k",
        ge=1,
        le=200,
        description="The top most relevant results to fetch at the retrieval step. The maximum number of results allowed is 200.",
    )
    fields: List[str] = SearchParamDefaults.fields.to_pydantic_field()
    filters: Union[List[str], List[Filter]] = Field(
        default=[],
        title="Filters",
        description="The list of filters to apply. Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters",  # noqa: E501
    )
    keyword_filters: Union[list[str], list[Filter]] = Field(
        default=[],
        title="Keyword filters",
        description=(
            "List of keyword filter expressions to apply to the retrieval step. "
            "The text block search will only be performed on the documents that contain the specified keywords. "
            "The filters are case-insensitive, and only alphanumeric characters and spaces are allowed. "
            "Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters"  # noqa
        ),
        examples=[
            ["NLP", "BERT"],
            [Filter(all=["NLP", "BERT"])],
            ["Friedrich Nietzsche", "Immanuel Kant"],
        ],
    )
    vectorset: Optional[str] = SearchParamDefaults.vectorset.to_pydantic_field()
    min_score: Optional[Union[float, MinScore]] = Field(
        default=None,
        title="Minimum score",
        description="Minimum score to filter search results. Results with a lower score will be ignored. Accepts either a float or a dictionary with the minimum scores for the bm25 and vector indexes. If a float is provided, it is interpreted as the minimum score for vector index search.",  # noqa
    )
    features: List[ChatOptions] = SearchParamDefaults.chat_features.to_pydantic_field()
    range_creation_start: Optional[DateTime] = (
        SearchParamDefaults.range_creation_start.to_pydantic_field()
    )
    range_creation_end: Optional[DateTime] = SearchParamDefaults.range_creation_end.to_pydantic_field()
    range_modification_start: Optional[DateTime] = (
        SearchParamDefaults.range_modification_start.to_pydantic_field()
    )
    range_modification_end: Optional[DateTime] = (
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
    citation_threshold: Optional[float] = Field(
        default=None,
        description="If citations is True, this sets the similarity threshold (0 to 1) for paragraphs to be included as citations. Lower values result in more citations. If not provided, Nuclia's default threshold is used.",
        ge=0.0,
        le=1.0,
    )
    security: Optional[RequestSecurity] = SearchParamDefaults.security.to_pydantic_field()
    show_hidden: SkipJsonSchema[bool] = SearchParamDefaults.show_hidden.to_pydantic_field()
    rag_strategies: list[RagStrategies] = Field(
        default=[],
        title="RAG context building strategies",
        description=(
            """Options for tweaking how the context for the LLM model is crafted:
- `full_resource` will add the full text of the matching resources to the context. This strategy cannot be combined with `hierarchy`, `neighbouring_paragraphs`, or `field_extension`.
- `field_extension` will add the text of the matching resource's specified fields to the context.
- `hierarchy` will add the title and summary text of the parent resource to the context for each matching paragraph.
- `neighbouring_paragraphs` will add the sorrounding paragraphs to the context for each matching paragraph.
- `metadata_extension` will add the metadata of the matching paragraphs or its resources to the context.
- `prequeries` allows to run multiple retrieval queries before the main query and add the results to the context. The results of specific queries can be boosted by the specifying weights.

If empty, the default strategy is used, which simply adds the text of the matching paragraphs to the context.
"""
        ),
        examples=[
            [{"name": "full_resource", "count": 2}],
            [
                {"name": "field_extension", "fields": ["t/amend", "a/title"]},
            ],
            [{"name": "hierarchy", "count": 2}],
            [{"name": "neighbouring_paragraphs", "before": 2, "after": 2}],
            [{"name": "metadata_extension", "types": ["origin", "classification_labels"]}],
            [
                {
                    "name": "prequeries",
                    "queries": [
                        {
                            "request": {
                                "query": "What is the capital of France?",
                                "features": ["keyword"],
                            },
                            "weight": 0.5,
                        },
                        {
                            "request": {
                                "query": "What is the capital of Germany?",
                            },
                            "weight": 0.5,
                        },
                    ],
                }
            ],
        ],
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
        description=(
            "Rephrase the query for a more efficient retrieval. This will consume LLM tokens and make the request slower."
        ),
    )

    prefer_markdown: bool = Field(
        default=False,
        title="Prefer markdown",
        description="If set to true, the response will be in markdown format",
    )

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

    @field_validator("rag_strategies", mode="before")
    @classmethod
    def validate_rag_strategies(cls, rag_strategies: list[RagStrategies]) -> list[RagStrategies]:
        strategy_names: set[str] = set()
        for strategy in rag_strategies or []:
            if isinstance(strategy, dict):
                obj = strategy
            elif isinstance(strategy, BaseModel):
                obj = strategy.model_dump()
            else:
                raise ValueError(
                    "RAG strategies must be defined using a valid RagStrategy object or a dictionary"
                )
            strategy_name = obj.get("name")
            if strategy_name is None:
                raise ValueError(f"Invalid strategy '{strategy}'")
            strategy_names.add(strategy_name)

        if len(strategy_names) != len(rag_strategies):
            raise ValueError("There must be at most one strategy of each type")

        for not_allowed_combination in (
            {RagStrategyName.FULL_RESOURCE, RagStrategyName.HIERARCHY},
            {RagStrategyName.FULL_RESOURCE, RagStrategyName.NEIGHBOURING_PARAGRAPHS},
            {RagStrategyName.FULL_RESOURCE, RagStrategyName.FIELD_EXTENSION},
        ):
            if not_allowed_combination.issubset(strategy_names):
                raise ValueError(
                    f"The following strategies cannot be combined in the same request: {', '.join(sorted(not_allowed_combination))}"
                )
        return rag_strategies

    @field_validator("features", mode="after")
    @classmethod
    def normalize_features(cls, features: List[ChatOptions]):
        return [feature.normalized() for feature in features]


# Alias (for backwards compatiblity with testbed)
class ChatRequest(AskRequest):
    pass


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
            SearchOptions.KEYWORD,
            SearchOptions.SEMANTIC,
        ]
    )

    @field_validator("features", mode="after")
    @classmethod
    def fulltext_not_supported(cls, v):
        # features are already normalized in the BaseSearchRequest model
        if SearchOptions.FULLTEXT in v or SearchOptions.FULLTEXT == v:
            raise ValueError("fulltext search not supported")
        return v

    keyword_filters: Union[list[str], list[Filter]] = Field(
        default=[],
        title="Keyword filters",
        description=(
            "List of keyword filter expressions to apply to the retrieval step. "
            "The text block search will only be performed on the documents that contain the specified keywords. "
            "The filters are case-insensitive, and only alphanumeric characters and spaces are allowed. "
            "Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters"  # noqa
        ),
        examples=[
            ["NLP", "BERT"],
            [Filter(all=["NLP", "BERT"])],
            ["Friedrich Nietzsche", "Immanuel Kant"],
        ],
    )


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
    page_number: int = Field(
        default=0, description="Pagination will be deprecated, please, refer to `top_k` in the request"
    )
    page_size: int = Field(
        default=20, description="Pagination will be deprecated, please, refer to `top_k` in the request"
    )
    next_page: bool = Field(
        default=False,
        description="Pagination will be deprecated, please, refer to `top_k` in the request",
    )
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

    def to_proto(self) -> int:
        return TaskType.Value(self.name)


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
        default=None,
        title="Generative first chunk",
        description="Time the LLM took to generate the first chunk of the answer",
    )
    generative_total: Optional[float] = Field(
        default=None,
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
    prequeries: Optional[Dict[str, KnowledgeboxFindResults]] = Field(
        default=None,
        title="Prequeries",
        description="The retrieval results of the prequeries",
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


class PrequeriesAskResponseItem(BaseModel):
    type: Literal["prequeries"] = "prequeries"
    results: dict[str, KnowledgeboxFindResults] = {}


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
    PrequeriesAskResponseItem,
]


class AskResponseItem(BaseModel):
    item: AskResponseItemType = Field(..., discriminator="type")


def parse_custom_prompt(item: AskRequest) -> CustomPrompt:
    prompt = CustomPrompt()
    if item.prompt is not None:
        if isinstance(item.prompt, str):
            # If the prompt is a string, it is interpreted as the user prompt
            prompt.user = item.prompt
        else:
            prompt.user = item.prompt.user
            prompt.system = item.prompt.system
            prompt.rephrase = item.prompt.rephrase
    return prompt


def parse_rephrase_prompt(item: AskRequest) -> Optional[str]:
    prompt = parse_custom_prompt(item)
    return prompt.rephrase
