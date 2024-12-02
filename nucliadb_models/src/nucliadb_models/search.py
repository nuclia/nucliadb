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
import json
from enum import Enum
from typing import Any, Literal, Optional, TypeVar, Union

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema
from typing_extensions import Annotated, Self, deprecated

from nucliadb_models.common import FieldTypeName, ParamDefault

# Bw/c import to avoid breaking users
# noqa isort: skip
from nucliadb_models.metadata import RelationType, ResourceProcessingStatus
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.security import RequestSecurity
from nucliadb_models.utils import DateTime
from nucliadb_protos.audit_pb2 import ClientType, TaskType
from nucliadb_protos.nodereader_pb2 import OrderBy
from nucliadb_protos.utils_pb2 import RelationNode

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


class ChatOptions(str, Enum):
    KEYWORD = "keyword"
    RELATIONS = "relations"
    SEMANTIC = "semantic"


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
    facetresults: dict[str, int]


FacetsResult = dict[str, Any]


class TextPosition(BaseModel):
    page_number: Optional[int] = None
    index: int
    start: int
    end: int
    start_seconds: Optional[list[int]] = None
    end_seconds: Optional[list[int]] = None


class Sentence(BaseModel):
    score: float
    rid: str
    text: str
    field_type: str
    field: str
    index: Optional[str] = None
    position: Optional[TextPosition] = None


class Sentences(BaseModel):
    results: list[Sentence] = []
    facets: FacetsResult
    page_number: int = 0
    page_size: int = 20
    min_score: float = Field(
        title="Minimum score",
        description="Minimum similarity score used to filter vector index search. Results with a lower score have been ignored.",  # noqa: E501
    )


class Paragraph(BaseModel):
    score: float
    rid: str
    field_type: str
    field: str
    text: str
    labels: list[str] = []
    start_seconds: Optional[list[int]] = None
    end_seconds: Optional[list[int]] = None
    position: Optional[TextPosition] = None
    fuzzy_result: bool = False


class Paragraphs(BaseModel):
    results: list[Paragraph] = []
    facets: Optional[FacetsResult] = None
    query: Optional[str] = None
    total: int = 0
    page_number: int = 0
    page_size: int = 20
    next_page: bool = False
    min_score: float = Field(
        title="Minimum score",
        description="Minimum bm25 score used to filter bm25 index search. Results with a lower score have been ignored.",  # noqa: E501
    )


class ResourceResult(BaseModel):
    score: Union[float, int]
    rid: str
    field_type: str
    field: str
    labels: Optional[list[str]] = None


class Resources(BaseModel):
    results: list[ResourceResult]
    facets: Optional[FacetsResult] = None
    query: Optional[str] = None
    total: int = 0
    page_number: int = 0
    page_size: int = 20
    next_page: bool = False
    min_score: float = Field(
        title="Minimum score",
        description="Minimum bm25 score used to filter bm25 index search. Results with a lower score have been ignored.",  # noqa: E501
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
    related_to: list[DirectionalRelation]


# TODO: uncomment and implement (next iteration)
# class RelationPath(BaseModel):
#     origin: str
#     destination: str
#     path: list[DirectionalRelation]


class Relations(BaseModel):
    entities: dict[str, EntitySubgraph]
    # TODO: implement in the next iteration of knowledge graph search
    # graph: list[RelationPath]


class RelatedEntity(BaseModel, frozen=True):
    family: str
    value: str


class RelatedEntities(BaseModel):
    total: int = 0
    entities: list[RelatedEntity] = []


class ResourceSearchResults(JsonBaseModel):
    """Search on resource results"""

    sentences: Optional[Sentences] = None
    paragraphs: Optional[Paragraphs] = None
    relations: Optional[Relations] = None
    nodes: Optional[list[dict[str, str]]] = None
    shards: Optional[list[str]] = None


class KnowledgeboxSearchResults(JsonBaseModel):
    """Search on knowledgebox results"""

    resources: dict[str, Resource] = {}
    sentences: Optional[Sentences] = None
    paragraphs: Optional[Paragraphs] = None
    fulltext: Optional[Resources] = None
    relations: Optional[Relations] = None
    nodes: Optional[list[dict[str, str]]] = None
    shards: Optional[list[str]] = None
    autofilters: list[str] = ModelParamDefaults.applied_autofilters.to_pydantic_field()


class CatalogResponse(BaseModel):
    """Catalog results"""

    resources: dict[str, Resource] = {}
    fulltext: Optional[Resources] = None
    shards: Optional[list[str]] = None


class KnowledgeboxSuggestResults(JsonBaseModel):
    """Suggest on resource results"""

    paragraphs: Optional[Paragraphs] = None
    entities: Optional[RelatedEntities] = None
    shards: Optional[list[str]] = None


class KnowledgeboxCounters(BaseModel):
    resources: int
    paragraphs: int
    fields: int
    sentences: int
    shards: Optional[list[str]] = None
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


class RankFusionName(str, Enum):
    RECIPROCAL_RANK_FUSION = "rrf"


class _BaseRankFusion(BaseModel):
    name: str


class ReciprocalRankFusionWeights(BaseModel):
    keyword: float = 1.0
    semantic: float = 1.0


class ReciprocalRankFusion(_BaseRankFusion):
    name: Literal[RankFusionName.RECIPROCAL_RANK_FUSION] = RankFusionName.RECIPROCAL_RANK_FUSION
    k: float = Field(
        default=60.0,
        title="RRF k parameter",
        description="k parameter changes the influence top-ranked and lower-ranked elements have. Research has shown that 60 is a performant value across datasets",  # noqa: E501
    )
    window: Optional[int] = Field(
        default=None,
        le=500,
        title="RRF window",
        description="Number of elements for retrieval to do RRF. Window must be greater or equal to top_k. Greater values will increase probability of multi match at cost of retrieval time",  # noqa: E501
    )
    boosting: ReciprocalRankFusionWeights = Field(
        default_factory=ReciprocalRankFusionWeights,
        title="Retrievers boosting",
        description="""\
Define different weights for each retriever. This allows to assign different priorities to different retrieval methods. RRF scores will be multiplied by this value.

The default is 1 for each retriever, which means no extra boost for any of them. Weights below 0 can be used for negative boosting.

This kind of boosting can be useful in multilingual search, for example, where keyword search may not give good results and can degrade the final search experience
        """,  # noqa: E501
    )


RankFusion = Annotated[
    Union[ReciprocalRankFusion],
    Field(discriminator="name"),
]


class RerankerName(str, Enum):
    """Rerankers

    - Multi-match booster (default, deprecated): given a set of results from different
      sources, e.g., keyword and semantic search boost results appearing in both
      sets

    - Predict reranker: after retrieval, send the results to Predict API to
      rerank it. This method uses a reranker model, so one can expect better
      results at the expense of more latency.

      This will be the new default

    - No-operation (noop) reranker: maintain order and do not rerank the results
      after retrieval

    """

    MULTI_MATCH_BOOSTER: Annotated[
        str,
        deprecated("We recommend switching to the new default predict reranker for far better results"),
    ] = "multi_match_booster"
    PREDICT_RERANKER = "predict"
    NOOP = "noop"


class _BaseReranker(BaseModel):
    name: str


class PredictReranker(_BaseReranker):
    name: Literal[RerankerName.PREDICT_RERANKER] = RerankerName.PREDICT_RERANKER
    window: Optional[int] = Field(
        default=None,
        le=200,
        title="Reranker window",
        description="Number of elements reranker will use. Window must be greater or equal to top_k. Greater values will improve results at cost of retrieval and reranking time. By default, this reranker uses a default of 2 times top_k",  # noqa: E501
    )


Reranker = Annotated[Union[PredictReranker], Field(discriminator="name")]


class KnowledgeBoxCount(BaseModel):
    paragraphs: int
    fields: int
    sentences: int


class SearchParamDefaults:
    query = ParamDefault(
        default="",
        title="Query",
        description="The query to search for",
        max_items=20_000,
    )
    suggest_query = ParamDefault(
        default=..., title="Query", description="The query to get suggestions for"
    )
    fields = ParamDefault(
        default=[],
        title="Fields",
        description=(
            "The list of fields to search in. For instance: `a/title` to search only on title field. "
            "For more details on filtering by field, see: https://docs.nuclia.dev/docs/rag/advanced/search/#search-in-a-specific-field. "
        ),
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
        max_items=20_000,
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
        description="Whether to return matches for custom knowledge box synonyms of the query terms. Note: only supported for `keyword` and `fulltext` search options.",  # noqa: E501
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
        description="List of search features to use. Each value corresponds to a lookup into on of the different indexes",
    )
    rank_fusion = ParamDefault(
        default=RankFusionName.RECIPROCAL_RANK_FUSION,
        title="Rank fusion",
        description="Rank fusion algorithm to use to merge results from multiple retrievers (keyword, semantic)",
    )
    reranker = ParamDefault(
        default=RerankerName.MULTI_MATCH_BOOSTER,
        title="Reranker",
        description="Reranker let you specify which method you want to use to rerank your results at the end of retrieval\nDEPRECATION! multi_match_booster will be deprecated and predict will be the new default",  # noqa: E501
    )
    debug = ParamDefault(
        default=False,
        title="Debug mode",
        description="If set, the response will include some extra metadata for debugging purposes, like the list of queried nodes.",  # noqa: E501
    )
    show = ParamDefault(
        default=[ResourceProperties.BASIC],
        title="Show metadata",
        description="Controls which types of metadata are serialized on resources of search results",
    )
    extracted = ParamDefault(
        default=[],
        title="Extracted metadata",
        description="[Deprecated] Please use GET resource endpoint instead to get extracted metadata",
        deprecated=True,
    )
    field_type_filter = ParamDefault(
        default=list(FieldTypeName),
        title="Field type filter",
        description="Define which field types are serialized on resources of search results",
    )
    range_creation_start = ParamDefault(
        default=None,
        title="Resource creation range start",
        description="Resources created before this date will be filtered out of search results. Datetime are represented as a str in ISO 8601 format, like: 2008-09-15T15:53:00+05:00.",  # noqa: E501
    )
    range_creation_end = ParamDefault(
        default=None,
        title="Resource creation range end",
        description="Resources created after this date will be filtered out of search results. Datetime are represented as a str in ISO 8601 format, like: 2008-09-15T15:53:00+05:00.",  # noqa: E501
    )
    range_modification_start = ParamDefault(
        default=None,
        title="Resource modification range start",
        description="Resources modified before this date will be filtered out of search results. Datetime are represented as a str in ISO 8601 format, like: 2008-09-15T15:53:00+05:00.",  # noqa: E501
    )
    range_modification_end = ParamDefault(
        default=None,
        title="Resource modification range end",
        description="Resources modified after this date will be filtered out of search results. Datetime are represented as a str in ISO 8601 format, like: 2008-09-15T15:53:00+05:00.",  # noqa: E501
    )
    vector = ParamDefault(
        default=None,
        title="Search Vector",
        description="The vector to perform the search with. If not provided, NucliaDB will use Nuclia Predict API to create the vector off from the query.",  # noqa: E501
    )
    vectorset = ParamDefault(
        default=None,
        title="Vectorset",
        description="Vectors index to perform the search in. If not provided, NucliaDB will use the default one",
    )
    chat_context = ParamDefault(
        default=None,
        title="Chat history",
        description="Use to rephrase the new LLM query by taking into account the chat conversation history",  # noqa: E501
    )
    chat_features = ParamDefault(
        default=[ChatOptions.SEMANTIC, ChatOptions.KEYWORD],
        title="Chat features",
        description="Features enabled for the chat endpoint. Semantic search is done if `semantic` is included. If `keyword` is included, the results will include matching paragraphs from the bm25 index. If `relations` is included, a graph of entities related to the answer is returned. `paragraphs` and `vectors` are deprecated, please use `keyword` and `semantic` instead",  # noqa: E501
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
        description="Security metadata for the request. If not provided, the search request is done without the security lookup phase.",  # noqa: E501
    )
    security_groups = ParamDefault(
        default=[],
        title="Security groups",
        description="List of security groups to filter search results for. Only resources matching the query and containing the specified security groups will be returned. If empty, all resources will be considered for the search.",  # noqa: E501
    )
    rephrase = ParamDefault(
        default=False,
        title="Rephrase query consuming LLMs",
        description="Rephrase query consuming LLMs - it will make the query slower",  # noqa: E501
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
    all: Optional[list[str]] = Field(default=None, min_length=1)
    any: Optional[list[str]] = Field(default=None, min_length=1)
    none: Optional[list[str]] = Field(default=None, min_length=1)
    not_all: Optional[list[str]] = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_filter(self) -> Self:
        if (self.all, self.any, self.none, self.not_all).count(None) != 3:
            raise ValueError("Only one of 'all', 'any', 'none' or 'not_all' can be set")
        return self


class CatalogRequest(BaseModel):
    query: str = SearchParamDefaults.query.to_pydantic_field()
    filters: Union[list[str], list[Filter]] = Field(
        default=[],
        title="Filters",
        description="The list of filters to apply. Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters",  # noqa: E501
    )
    faceted: list[str] = SearchParamDefaults.faceted.to_pydantic_field()
    sort: Optional[SortOptions] = SearchParamDefaults.sort.to_pydantic_field()
    page_number: int = SearchParamDefaults.catalog_page_number.to_pydantic_field()
    page_size: int = SearchParamDefaults.catalog_page_size.to_pydantic_field()
    shards: list[str] = SearchParamDefaults.shards.to_pydantic_field(deprecated=True)
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
    hidden: Optional[bool] = SearchParamDefaults.hidden.to_pydantic_field()

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


AUDIT_METADATA_MAX_BYTES = 1024 * 10  # 10KB


class AuditMetadataBase(BaseModel):
    audit_metadata: Optional[dict[str, str]] = Field(
        default=None,
        title="Audit metadata",
        description=(
            "A dictionary containing optional audit-specific metadata, such as user_id, environment, or other contextual information."
            " This metadata can be leveraged for filtering and analyzing activity logs in future operations."
            " Each key-value pair represents a piece of metadata relevant to the user's request."
        ),
        examples=[{"environment": "test", "user": "my-user-123"}],
    )

    @field_validator("audit_metadata", mode="after")
    def check_audit_metadata_size(cls, value):
        if value:
            size = len(json.dumps(value).encode("utf-8"))
            if size > AUDIT_METADATA_MAX_BYTES:
                raise ValueError(
                    f"Audit metadata size is too large: {size} bytes. Maximum size allowed: {AUDIT_METADATA_MAX_BYTES}"
                )
        return value


class BaseSearchRequest(AuditMetadataBase):
    query: str = SearchParamDefaults.query.to_pydantic_field()
    fields: list[str] = SearchParamDefaults.fields.to_pydantic_field()
    filters: Union[list[str], list[Filter]] = Field(
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
        description="Minimum score to filter search results. Results with a lower score will be ignored. Accepts either a float or a dictionary with the minimum scores for the bm25 and vector indexes. If a float is provided, it is interpreted as the minimum score for vector index search.",  # noqa: E501
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
    features: list[SearchOptions] = SearchParamDefaults.search_features.to_pydantic_field(
        default=[
            SearchOptions.KEYWORD,
            SearchOptions.FULLTEXT,
            SearchOptions.SEMANTIC,
        ]
    )
    debug: bool = SearchParamDefaults.debug.to_pydantic_field()
    highlight: bool = SearchParamDefaults.highlight.to_pydantic_field()
    show: list[ResourceProperties] = SearchParamDefaults.show.to_pydantic_field()
    field_type_filter: list[FieldTypeName] = SearchParamDefaults.field_type_filter.to_pydantic_field()
    extracted: list[ExtractedDataTypeName] = SearchParamDefaults.extracted.to_pydantic_field()
    shards: list[str] = SearchParamDefaults.shards.to_pydantic_field()
    vector: Optional[list[float]] = SearchParamDefaults.vector.to_pydantic_field()
    vectorset: Optional[str] = SearchParamDefaults.vectorset.to_pydantic_field()
    with_duplicates: bool = SearchParamDefaults.with_duplicates.to_pydantic_field()
    with_synonyms: bool = SearchParamDefaults.with_synonyms.to_pydantic_field()
    autofilter: bool = SearchParamDefaults.autofilter.to_pydantic_field()
    resource_filters: list[str] = SearchParamDefaults.resource_filters.to_pydantic_field()
    security: Optional[RequestSecurity] = SearchParamDefaults.security.to_pydantic_field()
    show_hidden: bool = SearchParamDefaults.show_hidden.to_pydantic_field()

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

    @model_validator(mode="after")
    def top_k_overwrites_pagination(self):
        """This method adds support for `top_k` attribute, overwriting
        `page_number` and `page_size` if needed"""
        if self.top_k is not None:
            self.page_number = 0
            self.page_size = self.top_k
        return self


class SearchRequest(BaseSearchRequest):
    faceted: list[str] = SearchParamDefaults.faceted.to_pydantic_field()
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
    query_context: dict[str, str] = Field(
        default={},
        description="The information retrieval context for the current query",
    )
    query_context_order: Optional[dict[str, int]] = Field(
        default=None,
        description="The order of the query context elements. This is used to sort the context elements by relevance before sending them to the generative model",  # noqa: E501
    )
    chat_history: list[ChatContextMessage] = Field(
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
        description="If citations is True, this sets the similarity threshold (0 to 1) for paragraphs to be included as citations. Lower values result in more citations. If not provided, Nuclia's default threshold is used.",  # noqa: E501
        ge=0.0,
        le=1.0,
    )
    generative_model: Optional[str] = Field(
        default=None,
        title="Generative model",
        description="The generative model to use for the predict chat endpoint. If not provided, the model configured for the Knowledge Box is used.",  # noqa: E501
    )

    max_tokens: Optional[int] = Field(default=None, description="Maximum characters to generate")

    query_context_images: dict[str, Image] = Field(
        default={},
        description="The information retrieval context for the current query, each image is a base64 encoded string",
    )

    prefer_markdown: bool = Field(
        default=False,
        description="If set to true, the response will be in markdown format",
    )
    json_schema: Optional[dict[str, Any]] = Field(
        default=None,
        description="The JSON schema to use for the generative model answers",
    )
    rerank_context: bool = Field(
        default=False,
        description="Whether to reorder the query context based on a reranker",
    )
    top_k: Optional[int] = Field(default=None, description="Number of best elements to get from")


class RephraseModel(BaseModel):
    question: str
    chat_history: list[ChatContextMessage] = []
    user_id: str
    user_context: list[str] = []
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
    CONVERSATION = "conversation"


class ImageRagStrategyName:
    PAGE_IMAGE = "page_image"
    TABLES = "tables"
    PARAGRAPH_IMAGE = "paragraph_image"


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
        description="List of field ids to extend the context with. It will try to extend the retrieval context with the specified fields in the matching resources. The field ids have to be in the format `{field_type}/{field_name}`, like 'a/title', 'a/summary' for title and summary fields or 't/amend' for a text field named 'amend'.",  # noqa: E501
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


class ConversationalStrategy(RagStrategy):
    name: Literal["conversation"] = "conversation"
    attachments_text: bool = Field(
        default=False,
        title="Add attachments on context",
        description="Add attachments on context retrieved on conversation",
    )
    attachments_images: bool = Field(
        default=False,
        title="Add attachments images on context",
        description="Add attachments images on context retrieved on conversation if they are mime type image and using a visual LLM",
    )
    full: bool = Field(
        default=False,
        title="Add all conversation",
        description="Add all conversation fields on matched blocks",
    )
    max_messages: int = Field(
        default=15,
        title="Max messages",
        description="Max messages to append in case its not full field",
        ge=0,
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
        max_length=15,
    )
    main_query_weight: float = Field(
        default=1.0,
        title="Main query weight",
        description="Weight of the main query in the context. Use this to control the importance of the main query in the context.",
        ge=0,
    )


PreQueryResult = tuple[PreQuery, "KnowledgeboxFindResults"]


class TableImageStrategy(ImageRagStrategy):
    name: Literal["tables"] = "tables"


class PageImageStrategy(ImageRagStrategy):
    name: Literal["page_image"] = "page_image"
    count: Optional[int] = Field(
        default=None,
        title="Count",
        description="Maximum number of images to retrieve from the page. By default, at most 5 images are retrieved.",
    )


class ParagraphImageStrategy(ImageRagStrategy):
    name: Literal["paragraph_image"] = "paragraph_image"


RagStrategies = Annotated[
    Union[
        FieldExtensionStrategy,
        FullResourceStrategy,
        HierarchyResourceStrategy,
        NeighbouringParagraphsStrategy,
        MetadataExtensionStrategy,
        ConversationalStrategy,
        PreQueriesStrategy,
    ],
    Field(discriminator="name"),
]
RagImagesStrategies = Annotated[
    Union[PageImageStrategy, ParagraphImageStrategy, TableImageStrategy],
    Field(discriminator="name"),
]
PromptContext = dict[str, str]
PromptContextOrder = dict[str, int]
PromptContextImages = dict[str, Image]


class CustomPrompt(BaseModel):
    system: Optional[str] = Field(
        default=None,
        title="System prompt",
        description="System prompt given to the generative model responsible of generating the answer. This can help customize the behavior of the model when generating the answer. If not specified, the default model provider's prompt is used.",  # noqa: E501
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
        description="User prompt given to the generative model responsible of generating the answer. Use the words {context} and {question} in brackets where you want those fields to be placed, in case you want them in your prompt. Context will be the data returned by the retrieval step and question will be the user's query.",  # noqa: E501
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


class AskRequest(AuditMetadataBase):
    query: str = SearchParamDefaults.chat_query.to_pydantic_field()
    top_k: int = Field(
        default=20,
        title="Top k",
        ge=1,
        le=200,
        description="The top most relevant results to fetch at the retrieval step. The maximum number of results allowed is 200.",
    )
    fields: list[str] = SearchParamDefaults.fields.to_pydantic_field()
    filters: Union[list[str], list[Filter]] = Field(
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
            "Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters"  # noqa: E501
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
        description="Minimum score to filter search results. Results with a lower score will be ignored. Accepts either a float or a dictionary with the minimum scores for the bm25 and vector indexes. If a float is provided, it is interpreted as the minimum score for vector index search.",  # noqa: E501
    )
    features: list[ChatOptions] = SearchParamDefaults.chat_features.to_pydantic_field()
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
    show: list[ResourceProperties] = SearchParamDefaults.show.to_pydantic_field()
    field_type_filter: list[FieldTypeName] = SearchParamDefaults.field_type_filter.to_pydantic_field()
    extracted: list[ExtractedDataTypeName] = SearchParamDefaults.extracted.to_pydantic_field()
    shards: list[str] = SearchParamDefaults.shards.to_pydantic_field()
    context: Optional[list[ChatContextMessage]] = SearchParamDefaults.chat_context.to_pydantic_field()
    extra_context: Optional[list[str]] = Field(
        default=None,
        title="Extra query context",
        description="""Additional context that is added to the retrieval context sent to the LLM.
        It allows extending the chat feature with content that may not be in the Knowledge Box.""",
    )
    autofilter: bool = SearchParamDefaults.autofilter.to_pydantic_field()
    highlight: bool = SearchParamDefaults.highlight.to_pydantic_field()
    resource_filters: list[str] = SearchParamDefaults.resource_filters.to_pydantic_field()
    prompt: Optional[Union[str, CustomPrompt]] = Field(
        default=None,
        title="Prompts",
        description="Use to customize the prompts given to the generative model. Both system and user prompts can be customized. If a string is provided, it is interpreted as the user prompt.",  # noqa: E501
    )
    rank_fusion: Union[RankFusionName, RankFusion] = SearchParamDefaults.rank_fusion.to_pydantic_field()
    reranker: Union[RerankerName, Reranker] = SearchParamDefaults.reranker.to_pydantic_field()
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
    show_hidden: bool = SearchParamDefaults.show_hidden.to_pydantic_field()
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
            [
                {
                    "name": "metadata_extension",
                    "types": ["origin", "classification_labels"],
                }
            ],
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
        description=(
            "Options for tweaking how the image based context for the LLM model is crafted:\n"
            "- `page_image` will add the full page image of the matching resources to the context.\n"
            "- `tables` will send the table images for the paragraphs that contain tables and matched the retrieval query.\n"
            "- `paragraph_image` will add the images of the paragraphs that contain images (images for tables are not included).\n"
            "No image strategy is used by default. Note that this is only available for LLM models that support visual inputs. If the model does not support visual inputs, the image strategies will be ignored."
        ),
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
        description="Use to limit the amount of tokens used in the LLM context and/or for generating the answer. If not provided, the default maximum tokens of the generative model will be used. If an integer is provided, it is interpreted as the maximum tokens for the answer.",  # noqa: E501
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

    answer_json_schema: Optional[dict[str, Any]] = Field(
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


# Alias (for backwards compatiblity with testbed)
class ChatRequest(AskRequest):
    pass


class SummarizeResourceModel(BaseModel):
    fields: dict[str, str] = {}


class SummaryKind(str, Enum):
    SIMPLE = "simple"
    EXTENDED = "extended"


class SummarizeModel(BaseModel):
    """
    Model for the summarize predict api request payload
    """

    resources: dict[str, SummarizeResourceModel] = {}
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

    resources: list[str] = Field(
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
    resources: dict[str, SummarizedResource] = Field(
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
    features: list[SearchOptions] = SearchParamDefaults.search_features.to_pydantic_field(
        default=[
            SearchOptions.KEYWORD,
            SearchOptions.SEMANTIC,
        ]
    )
    rank_fusion: Union[RankFusionName, RankFusion] = SearchParamDefaults.rank_fusion.to_pydantic_field()
    reranker: Union[RerankerName, Reranker] = SearchParamDefaults.reranker.to_pydantic_field()

    keyword_filters: Union[list[str], list[Filter]] = Field(
        default=[],
        title="Keyword filters",
        description=(
            "List of keyword filter expressions to apply to the retrieval step. "
            "The text block search will only be performed on the documents that contain the specified keywords. "
            "The filters are case-insensitive, and only alphanumeric characters and spaces are allowed. "
            "Filtering examples can be found here: https://docs.nuclia.dev/docs/rag/advanced/search/#filters"  # noqa: E501
        ),
        examples=[
            ["NLP", "BERT"],
            [Filter(all=["NLP", "BERT"])],
            ["Friedrich Nietzsche", "Immanuel Kant"],
        ],
    )

    @field_validator("features", mode="after")
    @classmethod
    def fulltext_not_supported(cls, v):
        # features are already normalized in the BaseSearchRequest model
        if SearchOptions.FULLTEXT in v or SearchOptions.FULLTEXT == v:
            raise ValueError("fulltext search not supported")
        return v


class SCORE_TYPE(str, Enum):
    VECTOR = "VECTOR"
    BM25 = "BM25"
    BOTH = "BOTH"
    RERANKER = "RERANKER"


class FindTextPosition(BaseModel):
    page_number: Optional[int] = None
    start_seconds: Optional[list[int]] = None
    end_seconds: Optional[list[int]] = None
    index: int
    start: int
    end: int


class FindParagraph(BaseModel):
    score: float
    score_type: SCORE_TYPE
    order: int = Field(default=0, ge=0)
    text: str
    id: str
    labels: Optional[list[str]] = []
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


class FindField(BaseModel):
    paragraphs: dict[str, FindParagraph]


class FindResource(Resource):
    fields: dict[str, FindField]

    def updated_from(self, origin: Resource):
        for key in origin.model_fields.keys():
            self.__setattr__(key, getattr(origin, key))


class KnowledgeboxFindResults(JsonBaseModel):
    """Find on knowledgebox results"""

    resources: dict[str, FindResource]
    relations: Optional[Relations] = None
    query: Optional[str] = None
    total: int = 0
    page_number: int = Field(
        default=0,
        description="Pagination will be deprecated, please, refer to `top_k` in the request",
    )
    page_size: int = Field(
        default=20,
        description="Pagination will be deprecated, please, refer to `top_k` in the request",
    )
    next_page: bool = Field(
        default=False,
        description="Pagination will be deprecated, please, refer to `top_k` in the request",
    )
    nodes: Optional[list[dict[str, str]]] = Field(
        default=None,
        title="Nodes",
        description="List of nodes queried in the search",
    )
    shards: Optional[list[str]] = Field(
        default=None,
        title="Shards",
        description="The list of shard replica ids used for the search.",
    )
    autofilters: list[str] = ModelParamDefaults.applied_autofilters.to_pydantic_field()
    min_score: Optional[Union[float, MinScore]] = Field(
        default=MinScore(),
        title="Minimum result score",
        description="The minimum scores that have been used for the search operation.",
    )
    best_matches: list[str] = Field(
        default=[],
        title="Best matches",
        description="List of ids of best matching paragraphs. The list is sorted by decreasing relevance (most relevant first).",  # noqa: E501
    )


class FeedbackTasks(str, Enum):
    CHAT = "CHAT"

    def to_proto(self) -> int:
        return TaskType.Value(self.name)


class FeedbackRequest(BaseModel):
    ident: str = Field(
        title="Request identifier",
        description="Id of the request to provide feedback for. This id is returned in the response header `Nuclia-Learning-Id` of the chat endpoint.",  # noqa: E501
    )
    good: bool = Field(title="Good", description="Whether the result was good or not")
    task: FeedbackTasks = Field(
        title="Task",
        description="The task the feedback is for. For now, only `CHAT` task is available",
    )
    feedback: Optional[str] = Field(None, title="Feedback", description="Feedback text")
    text_block_id: Optional[str] = Field(None, title="Text block", description="Text block id")


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


class AskRetrievalMatch(BaseModel):
    id: str = Field(
        title="Id",
        description="Id of the matching text block",
    )


class SyncAskResponse(BaseModel):
    answer: str = Field(
        title="Answer",
        description="The generative answer to the query",
    )
    answer_json: Optional[dict[str, Any]] = Field(
        default=None,
        title="Answer JSON",
        description="The generative JSON answer to the query. This is returned only if the answer_json_schema parameter is provided in the request.",  # noqa: E501
    )
    status: str = Field(
        title="Status",
        description="The status of the query execution. It can be 'success', 'error' or 'no_context'",  # noqa: E501
    )
    retrieval_results: KnowledgeboxFindResults = Field(
        title="Retrieval results",
        description="The retrieval results of the query",
    )
    retrieval_best_matches: list[AskRetrievalMatch] = Field(
        default=[],
        title="Retrieval best matches",
        description="Sorted list of best matching text blocks in the retrieval step. This includes the main query and prequeries results, if any.",
    )
    prequeries: Optional[dict[str, KnowledgeboxFindResults]] = Field(
        default=None,
        title="Prequeries",
        description="The retrieval results of the prequeries",
    )
    learning_id: str = Field(
        default="",
        title="Learning id",
        description="The id of the learning request. This id can be used to provide feedback on the learning process.",  # noqa: E501
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
        description="Metadata of the query execution. This includes the number of tokens used in the LLM context and answer, and the timings of the generative model.",  # noqa: E501
    )
    error_details: Optional[str] = Field(
        default=None,
        title="Error details",
        description="Error details message in case there was an error",
    )


class RetrievalAskResponseItem(BaseModel):
    type: Literal["retrieval"] = "retrieval"
    results: KnowledgeboxFindResults
    best_matches: list[AskRetrievalMatch] = Field(
        default=[],
        title="Best matches",
        description="Sorted list of best matching text blocks in the retrieval step. This includes the main query and prequeries results, if any.",
    )


class PrequeriesAskResponseItem(BaseModel):
    type: Literal["prequeries"] = "prequeries"
    results: dict[str, KnowledgeboxFindResults] = {}


class AnswerAskResponseItem(BaseModel):
    type: Literal["answer"] = "answer"
    text: str


class JSONAskResponseItem(BaseModel):
    type: Literal["answer_json"] = "answer_json"
    object: dict[str, Any]


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
