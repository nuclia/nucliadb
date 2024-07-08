# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.2.6.2](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.3
# Pydantic Version: 1.10.14
import typing
from datetime import datetime
from enum import IntEnum

from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel, Field

from .noderesources_p2p import (
    ParagraphMetadata,
    ResourceStatus,
    SentenceMetadata,
    ShardId,
)
from .utils_p2p import NodeType, Relation, RelationNode, RelationType, Security


class SuggestFeatures(IntEnum):
    ENTITIES = 0
    PARAGRAPHS = 1


class Filter(BaseModel):
    field_labels: typing.List[str] = Field(default_factory=list)
    paragraph_labels: typing.List[str] = Field(default_factory=list)
    expression: str = Field(default="")


class StreamFilter(BaseModel):
    class Conjunction(IntEnum):
        AND = 0
        OR = 1
        NOT = 2

    conjunction: "StreamFilter.Conjunction" = Field(default=0)
    labels: typing.List[str] = Field(default_factory=list)


class Faceted(BaseModel):
    labels: typing.List[str] = Field(default_factory=list)


class OrderBy(BaseModel):
    class OrderType(IntEnum):
        DESC = 0
        ASC = 1

    class OrderField(IntEnum):
        CREATED = 0
        MODIFIED = 1

    field: str = Field(default="")
    type: "OrderBy.OrderType" = Field(default=0)
    sort_by: "OrderBy.OrderField" = Field(default=0)


class Timestamps(BaseModel):
    from_modified: datetime = Field(default_factory=datetime.now)
    to_modified: datetime = Field(default_factory=datetime.now)
    from_created: datetime = Field(default_factory=datetime.now)
    to_created: datetime = Field(default_factory=datetime.now)


class FacetResult(BaseModel):
    tag: str = Field(default="")
    total: int = Field(default=0)


class FacetResults(BaseModel):
    facetresults: typing.List[FacetResult] = Field(default_factory=list)


class DocumentSearchRequest(BaseModel):
    id: str = Field(default="")
    body: str = Field(default="")
    fields: typing.List[str] = Field(default_factory=list)
    filter: Filter = Field()
    order: OrderBy = Field()
    faceted: Faceted = Field()
    page_number: int = Field(default=0)
    result_per_page: int = Field(default=0)
    timestamps: Timestamps = Field()
    reload: bool = Field(default=False)
    only_faceted: bool = Field(default=False)
    with_status: typing.Optional[ResourceStatus] = Field(default=0)
    advanced_query: typing.Optional[str] = Field(default="")
    min_score: float = Field(default=0.0)


class ParagraphSearchRequest(BaseModel):
    id: str = Field(default="")
    uuid: str = Field(default="")
    fields: typing.List[str] = Field(default_factory=list)
    body: str = Field(default="")
    filter: Filter = Field()
    order: OrderBy = Field()
    faceted: Faceted = Field()
    page_number: int = Field(default=0)
    result_per_page: int = Field(default=0)
    timestamps: Timestamps = Field()
    reload: bool = Field(default=False)
    with_duplicates: bool = Field(default=False)
    only_faceted: bool = Field(default=False)
    advanced_query: typing.Optional[str] = Field(default="")
    key_filters: typing.List[str] = Field(default_factory=list)
    min_score: float = Field(default=0.0)
    security: typing.Optional[Security] = Field(default=None)


class ResultScore(BaseModel):
    bm25: float = Field(default=0.0)
    booster: float = Field(default=0.0)


class DocumentResult(BaseModel):
    uuid: str = Field(default="")
    score: ResultScore = Field()
    field: str = Field(default="")
    labels: typing.List[str] = Field(default_factory=list)


class DocumentSearchResponse(BaseModel):
    total: int = Field(default=0)
    results: typing.List[DocumentResult] = Field(default_factory=list)
    facets: typing.Dict[str, FacetResults] = Field(default_factory=dict)
    page_number: int = Field(default=0)
    result_per_page: int = Field(default=0)
    query: str = Field(default="")
    next_page: bool = Field(default=False)
    bm25: bool = Field(default=False)


class ParagraphResult(BaseModel):
    uuid: str = Field(default="")
    field: str = Field(default="")
    start: int = Field(default=0)
    end: int = Field(default=0)
    paragraph: str = Field(default="")
    split: str = Field(default="")
    index: int = Field(default=0)
    score: ResultScore = Field()
    matches: typing.List[str] = Field(default_factory=list)
    metadata: ParagraphMetadata = Field()
    labels: typing.List[str] = Field(default_factory=list)


class ParagraphSearchResponse(BaseModel):
    fuzzy_distance: int = Field(default=0)
    total: int = Field(default=0)
    results: typing.List[ParagraphResult] = Field(default_factory=list)
    facets: typing.Dict[str, FacetResults] = Field(default_factory=dict)
    page_number: int = Field(default=0)
    result_per_page: int = Field(default=0)
    query: str = Field(default="")
    next_page: bool = Field(default=False)
    bm25: bool = Field(default=False)
    ematches: typing.List[str] = Field(default_factory=list)


class VectorSearchRequest(BaseModel):
    id: str = Field(default="")
    vector: typing.List[float] = Field(default_factory=list)
    field_labels: typing.List[str] = Field(default_factory=list)
    paragraph_labels: typing.List[str] = Field(default_factory=list)
    page_number: int = Field(default=0)
    result_per_page: int = Field(default=0)
    reload: bool = Field(default=False)
    with_duplicates: bool = Field(default=False)
    vector_set: str = Field(default="")
    key_filters: typing.List[str] = Field(default_factory=list)
    min_score: float = Field(default=0.0)


class DocumentVectorIdentifier(BaseModel):
    id: str = Field(default="")


class DocumentScored(BaseModel):
    doc_id: DocumentVectorIdentifier = Field()
    score: float = Field(default=0.0)
    metadata: SentenceMetadata = Field()
    labels: typing.List[str] = Field(default_factory=list)


class VectorSearchResponse(BaseModel):
    documents: typing.List[DocumentScored] = Field(default_factory=list)
    page_number: int = Field(default=0)
    result_per_page: int = Field(default=0)


class RelationNodeFilter(BaseModel):
    node_type: NodeType = Field(default=0)
    node_subtype: typing.Optional[str] = Field(default="")


class RelationEdgeFilter(BaseModel):
    relation_type: RelationType = Field(default=0)
    relation_subtype: typing.Optional[str] = Field(default="")
    relation_value: typing.List[str] = Field(default_factory=list)


class RelationPrefixSearchRequest(BaseModel):
    prefix: str = Field(default="")
    node_filters: typing.List[RelationNodeFilter] = Field(default_factory=list)


class RelationPrefixSearchResponse(BaseModel):
    nodes: typing.List[RelationNode] = Field(default_factory=list)


class EntitiesSubgraphRequest(BaseModel):
    class DeletedEntities(BaseModel):
        node_subtype: str = Field(default="")
        node_values: typing.List[str] = Field(default_factory=list)

    entry_points: typing.List[RelationNode] = Field(default_factory=list)
    depth: typing.Optional[int] = Field(default=0)
    deleted_entities: typing.List["EntitiesSubgraphRequest.DeletedEntities"] = Field(
        default_factory=list
    )
    deleted_groups: typing.List[str] = Field(default_factory=list)


class EntitiesSubgraphResponse(BaseModel):
    relations: typing.List[Relation] = Field(default_factory=list)


class RelationSearchRequest(BaseModel):
    shard_id: str = Field(default="")
    reload: bool = Field(default=False)
    prefix: RelationPrefixSearchRequest = Field()
    subgraph: EntitiesSubgraphRequest = Field()


class RelationSearchResponse(BaseModel):
    prefix: RelationPrefixSearchResponse = Field()
    subgraph: EntitiesSubgraphResponse = Field()


class SearchRequest(BaseModel):
    shard: str = Field(default="")
    fields: typing.List[str] = Field(default_factory=list)
    body: str = Field(default="")
    filter: Filter = Field()
    order: OrderBy = Field()
    faceted: Faceted = Field()
    page_number: int = Field(default=0)
    result_per_page: int = Field(default=0)
    timestamps: Timestamps = Field()
    vector: typing.List[float] = Field(default_factory=list)
    vectorset: str = Field(default="")
    reload: bool = Field(default=False)
    paragraph: bool = Field(default=False)
    document: bool = Field(default=False)
    with_duplicates: bool = Field(default=False)
    only_faceted: bool = Field(default=False)
    advanced_query: typing.Optional[str] = Field(default="")
    with_status: typing.Optional[ResourceStatus] = Field(default=0)
    relations: RelationSearchRequest = Field()
    relation_prefix: RelationPrefixSearchRequest = Field()
    relation_subgraph: EntitiesSubgraphRequest = Field()
    key_filters: typing.List[str] = Field(default_factory=list)
    min_score_semantic: float = Field(default=0.0)
    min_score_bm25: float = Field(default=0.0)
    security: typing.Optional[Security] = Field(default=None)


class SuggestRequest(BaseModel):
    shard: str = Field(default="")
    body: str = Field(default="")
    features: typing.List[SuggestFeatures] = Field(default_factory=list)
    filter: Filter = Field()
    timestamps: Timestamps = Field()
    fields: typing.List[str] = Field(default_factory=list)
    key_filters: typing.List[str] = Field(default_factory=list)


class RelatedEntities(BaseModel):
    entities: typing.List[str] = Field(default_factory=list)
    total: int = Field(default=0)


class SuggestResponse(BaseModel):
    total: int = Field(default=0)
    results: typing.List[ParagraphResult] = Field(default_factory=list)
    query: str = Field(default="")
    ematches: typing.List[str] = Field(default_factory=list)
    entity_results: RelationPrefixSearchResponse = Field()


class SearchResponse(BaseModel):
    document: DocumentSearchResponse = Field()
    paragraph: ParagraphSearchResponse = Field()
    vector: VectorSearchResponse = Field()
    relation: RelationSearchResponse = Field()


class IdCollection(BaseModel):
    ids: typing.List[str] = Field(default_factory=list)


class RelationEdge(BaseModel):
    edge_type: RelationType = Field(default=0)
    property: str = Field(default="")


class EdgeList(BaseModel):
    list: typing.List[RelationEdge] = Field(default_factory=list)


class GetShardRequest(BaseModel):
    shard_id: ShardId = Field()
    vectorset: str = Field(default="")


class ParagraphItem(BaseModel):
    id: str = Field(default="")
    labels: typing.List[str] = Field(default_factory=list)


class DocumentItem(BaseModel):
    uuid: str = Field(default="")
    field: str = Field(default="")
    labels: typing.List[str] = Field(default_factory=list)


class StreamRequest(BaseModel):
    filter__deprecated: Filter = Field()
    reload: bool = Field(default=False)
    shard_id: ShardId = Field()
    filter: StreamFilter = Field()


class GetShardFilesRequest(BaseModel):
    shard_id: str = Field(default="")


class ShardFile(BaseModel):
    relative_path: str = Field(default="")
    size: int = Field(default=0)


class ShardFileList(BaseModel):
    files: typing.List[ShardFile] = Field(default_factory=list)


class DownloadShardFileRequest(BaseModel):
    shard_id: str = Field(default="")
    relative_path: str = Field(default="")


class ShardFileChunk(BaseModel):
    data: bytes = Field(default=b"")
    index: int = Field(default=0)
