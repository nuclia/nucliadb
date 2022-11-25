"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import google.protobuf.timestamp_pb2
import nucliadb_protos.noderesources_pb2
import nucliadb_protos.utils_pb2
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions
from nucliadb_protos.noderesources_pb2 import (
    EmptyQuery as EmptyQuery,
    EmptyResponse as EmptyResponse,
    IndexMetadata as IndexMetadata,
    IndexParagraph as IndexParagraph,
    IndexParagraphs as IndexParagraphs,
    ParagraphMetadata as ParagraphMetadata,
    ParagraphPosition as ParagraphPosition,
    Resource as Resource,
    ResourceID as ResourceID,
    Shard as Shard,
    ShardCleaned as ShardCleaned,
    ShardCreated as ShardCreated,
    ShardId as ShardId,
    ShardIds as ShardIds,
    ShardList as ShardList,
    TextInformation as TextInformation,
    VectorSentence as VectorSentence,
)
from nucliadb_protos.utils_pb2 import (
    ExtractedText as ExtractedText,
    JoinGraph as JoinGraph,
    JoinGraphCnx as JoinGraphCnx,
    Relation as Relation,
    RelationNode as RelationNode,
    Vector as Vector,
    VectorObject as VectorObject,
    Vectors as Vectors,
)

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class Filter(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TAGS_FIELD_NUMBER: builtins.int
    @property
    def tags(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    def __init__(
        self,
        *,
        tags: collections.abc.Iterable[builtins.str] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["tags", b"tags"]) -> None: ...

global___Filter = Filter

@typing_extensions.final
class Faceted(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TAGS_FIELD_NUMBER: builtins.int
    @property
    def tags(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    def __init__(
        self,
        *,
        tags: collections.abc.Iterable[builtins.str] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["tags", b"tags"]) -> None: ...

global___Faceted = Faceted

@typing_extensions.final
class OrderBy(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class _OrderType:
        ValueType = typing.NewType("ValueType", builtins.int)
        V: typing_extensions.TypeAlias = ValueType

    class _OrderTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[OrderBy._OrderType.ValueType], builtins.type):  # noqa: F821
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        DESC: OrderBy._OrderType.ValueType  # 0
        ASC: OrderBy._OrderType.ValueType  # 1

    class OrderType(_OrderType, metaclass=_OrderTypeEnumTypeWrapper): ...
    DESC: OrderBy.OrderType.ValueType  # 0
    ASC: OrderBy.OrderType.ValueType  # 1

    FIELD_FIELD_NUMBER: builtins.int
    TYPE_FIELD_NUMBER: builtins.int
    field: builtins.str
    type: global___OrderBy.OrderType.ValueType
    def __init__(
        self,
        *,
        field: builtins.str = ...,
        type: global___OrderBy.OrderType.ValueType = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["field", b"field", "type", b"type"]) -> None: ...

global___OrderBy = OrderBy

@typing_extensions.final
class Timestamps(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FROM_MODIFIED_FIELD_NUMBER: builtins.int
    TO_MODIFIED_FIELD_NUMBER: builtins.int
    FROM_CREATED_FIELD_NUMBER: builtins.int
    TO_CREATED_FIELD_NUMBER: builtins.int
    @property
    def from_modified(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    @property
    def to_modified(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    @property
    def from_created(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    @property
    def to_created(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    def __init__(
        self,
        *,
        from_modified: google.protobuf.timestamp_pb2.Timestamp | None = ...,
        to_modified: google.protobuf.timestamp_pb2.Timestamp | None = ...,
        from_created: google.protobuf.timestamp_pb2.Timestamp | None = ...,
        to_created: google.protobuf.timestamp_pb2.Timestamp | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["from_created", b"from_created", "from_modified", b"from_modified", "to_created", b"to_created", "to_modified", b"to_modified"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["from_created", b"from_created", "from_modified", b"from_modified", "to_created", b"to_created", "to_modified", b"to_modified"]) -> None: ...

global___Timestamps = Timestamps

@typing_extensions.final
class FacetResult(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TAG_FIELD_NUMBER: builtins.int
    TOTAL_FIELD_NUMBER: builtins.int
    tag: builtins.str
    total: builtins.int
    def __init__(
        self,
        *,
        tag: builtins.str = ...,
        total: builtins.int = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["tag", b"tag", "total", b"total"]) -> None: ...

global___FacetResult = FacetResult

@typing_extensions.final
class FacetResults(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    FACETRESULTS_FIELD_NUMBER: builtins.int
    @property
    def facetresults(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___FacetResult]: ...
    def __init__(
        self,
        *,
        facetresults: collections.abc.Iterable[global___FacetResult] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["facetresults", b"facetresults"]) -> None: ...

global___FacetResults = FacetResults

@typing_extensions.final
class DocumentSearchRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    BODY_FIELD_NUMBER: builtins.int
    FIELDS_FIELD_NUMBER: builtins.int
    FILTER_FIELD_NUMBER: builtins.int
    ORDER_FIELD_NUMBER: builtins.int
    FACETED_FIELD_NUMBER: builtins.int
    PAGE_NUMBER_FIELD_NUMBER: builtins.int
    RESULT_PER_PAGE_FIELD_NUMBER: builtins.int
    TIMESTAMPS_FIELD_NUMBER: builtins.int
    RELOAD_FIELD_NUMBER: builtins.int
    id: builtins.str
    body: builtins.str
    @property
    def fields(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def filter(self) -> global___Filter: ...
    @property
    def order(self) -> global___OrderBy: ...
    @property
    def faceted(self) -> global___Faceted: ...
    page_number: builtins.int
    result_per_page: builtins.int
    @property
    def timestamps(self) -> global___Timestamps: ...
    reload: builtins.bool
    def __init__(
        self,
        *,
        id: builtins.str = ...,
        body: builtins.str = ...,
        fields: collections.abc.Iterable[builtins.str] | None = ...,
        filter: global___Filter | None = ...,
        order: global___OrderBy | None = ...,
        faceted: global___Faceted | None = ...,
        page_number: builtins.int = ...,
        result_per_page: builtins.int = ...,
        timestamps: global___Timestamps | None = ...,
        reload: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["faceted", b"faceted", "filter", b"filter", "order", b"order", "timestamps", b"timestamps"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["body", b"body", "faceted", b"faceted", "fields", b"fields", "filter", b"filter", "id", b"id", "order", b"order", "page_number", b"page_number", "reload", b"reload", "result_per_page", b"result_per_page", "timestamps", b"timestamps"]) -> None: ...

global___DocumentSearchRequest = DocumentSearchRequest

@typing_extensions.final
class ParagraphSearchRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    UUID_FIELD_NUMBER: builtins.int
    FIELDS_FIELD_NUMBER: builtins.int
    BODY_FIELD_NUMBER: builtins.int
    FILTER_FIELD_NUMBER: builtins.int
    ORDER_FIELD_NUMBER: builtins.int
    FACETED_FIELD_NUMBER: builtins.int
    PAGE_NUMBER_FIELD_NUMBER: builtins.int
    RESULT_PER_PAGE_FIELD_NUMBER: builtins.int
    TIMESTAMPS_FIELD_NUMBER: builtins.int
    RELOAD_FIELD_NUMBER: builtins.int
    WITH_DUPLICATES_FIELD_NUMBER: builtins.int
    id: builtins.str
    uuid: builtins.str
    @property
    def fields(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    body: builtins.str
    """query this text in all the paragraphs"""
    @property
    def filter(self) -> global___Filter: ...
    @property
    def order(self) -> global___OrderBy: ...
    @property
    def faceted(self) -> global___Faceted:
        """Faceted{ tags: Vec<String>}"""
    page_number: builtins.int
    result_per_page: builtins.int
    @property
    def timestamps(self) -> global___Timestamps: ...
    reload: builtins.bool
    with_duplicates: builtins.bool
    def __init__(
        self,
        *,
        id: builtins.str = ...,
        uuid: builtins.str = ...,
        fields: collections.abc.Iterable[builtins.str] | None = ...,
        body: builtins.str = ...,
        filter: global___Filter | None = ...,
        order: global___OrderBy | None = ...,
        faceted: global___Faceted | None = ...,
        page_number: builtins.int = ...,
        result_per_page: builtins.int = ...,
        timestamps: global___Timestamps | None = ...,
        reload: builtins.bool = ...,
        with_duplicates: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["faceted", b"faceted", "filter", b"filter", "order", b"order", "timestamps", b"timestamps"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["body", b"body", "faceted", b"faceted", "fields", b"fields", "filter", b"filter", "id", b"id", "order", b"order", "page_number", b"page_number", "reload", b"reload", "result_per_page", b"result_per_page", "timestamps", b"timestamps", "uuid", b"uuid", "with_duplicates", b"with_duplicates"]) -> None: ...

global___ParagraphSearchRequest = ParagraphSearchRequest

@typing_extensions.final
class ResultScore(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    BM25_FIELD_NUMBER: builtins.int
    BOOSTER_FIELD_NUMBER: builtins.int
    bm25: builtins.float
    booster: builtins.float
    """In the case of two equal bm25 scores, booster 
    decides
    """
    def __init__(
        self,
        *,
        bm25: builtins.float = ...,
        booster: builtins.float = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["bm25", b"bm25", "booster", b"booster"]) -> None: ...

global___ResultScore = ResultScore

@typing_extensions.final
class DocumentResult(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    UUID_FIELD_NUMBER: builtins.int
    SCORE_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    uuid: builtins.str
    @property
    def score(self) -> global___ResultScore: ...
    field: builtins.str
    def __init__(
        self,
        *,
        uuid: builtins.str = ...,
        score: global___ResultScore | None = ...,
        field: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["score", b"score"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["field", b"field", "score", b"score", "uuid", b"uuid"]) -> None: ...

global___DocumentResult = DocumentResult

@typing_extensions.final
class DocumentSearchResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class FacetsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        @property
        def value(self) -> global___FacetResults: ...
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: global___FacetResults | None = ...,
        ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value", b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

    TOTAL_FIELD_NUMBER: builtins.int
    RESULTS_FIELD_NUMBER: builtins.int
    FACETS_FIELD_NUMBER: builtins.int
    PAGE_NUMBER_FIELD_NUMBER: builtins.int
    RESULT_PER_PAGE_FIELD_NUMBER: builtins.int
    QUERY_FIELD_NUMBER: builtins.int
    NEXT_PAGE_FIELD_NUMBER: builtins.int
    BM25_FIELD_NUMBER: builtins.int
    total: builtins.int
    @property
    def results(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___DocumentResult]: ...
    @property
    def facets(self) -> google.protobuf.internal.containers.MessageMap[builtins.str, global___FacetResults]: ...
    page_number: builtins.int
    result_per_page: builtins.int
    query: builtins.str
    """The text that lead to this results"""
    next_page: builtins.bool
    """Is there a next page"""
    bm25: builtins.bool
    def __init__(
        self,
        *,
        total: builtins.int = ...,
        results: collections.abc.Iterable[global___DocumentResult] | None = ...,
        facets: collections.abc.Mapping[builtins.str, global___FacetResults] | None = ...,
        page_number: builtins.int = ...,
        result_per_page: builtins.int = ...,
        query: builtins.str = ...,
        next_page: builtins.bool = ...,
        bm25: builtins.bool = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["bm25", b"bm25", "facets", b"facets", "next_page", b"next_page", "page_number", b"page_number", "query", b"query", "result_per_page", b"result_per_page", "results", b"results", "total", b"total"]) -> None: ...

global___DocumentSearchResponse = DocumentSearchResponse

@typing_extensions.final
class ParagraphResult(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    UUID_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    START_FIELD_NUMBER: builtins.int
    END_FIELD_NUMBER: builtins.int
    PARAGRAPH_FIELD_NUMBER: builtins.int
    SPLIT_FIELD_NUMBER: builtins.int
    INDEX_FIELD_NUMBER: builtins.int
    SCORE_FIELD_NUMBER: builtins.int
    MATCHES_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    uuid: builtins.str
    field: builtins.str
    start: builtins.int
    end: builtins.int
    paragraph: builtins.str
    split: builtins.str
    index: builtins.int
    @property
    def score(self) -> global___ResultScore: ...
    @property
    def matches(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def metadata(self) -> nucliadb_protos.noderesources_pb2.ParagraphMetadata:
        """Metadata that can't be searched with but is returned on search results"""
    def __init__(
        self,
        *,
        uuid: builtins.str = ...,
        field: builtins.str = ...,
        start: builtins.int = ...,
        end: builtins.int = ...,
        paragraph: builtins.str = ...,
        split: builtins.str = ...,
        index: builtins.int = ...,
        score: global___ResultScore | None = ...,
        matches: collections.abc.Iterable[builtins.str] | None = ...,
        metadata: nucliadb_protos.noderesources_pb2.ParagraphMetadata | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata", b"metadata", "score", b"score"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["end", b"end", "field", b"field", "index", b"index", "matches", b"matches", "metadata", b"metadata", "paragraph", b"paragraph", "score", b"score", "split", b"split", "start", b"start", "uuid", b"uuid"]) -> None: ...

global___ParagraphResult = ParagraphResult

@typing_extensions.final
class ParagraphSearchResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class FacetsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.str
        @property
        def value(self) -> global___FacetResults: ...
        def __init__(
            self,
            *,
            key: builtins.str = ...,
            value: global___FacetResults | None = ...,
        ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value", b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key", b"key", "value", b"value"]) -> None: ...

    FUZZY_DISTANCE_FIELD_NUMBER: builtins.int
    TOTAL_FIELD_NUMBER: builtins.int
    RESULTS_FIELD_NUMBER: builtins.int
    FACETS_FIELD_NUMBER: builtins.int
    PAGE_NUMBER_FIELD_NUMBER: builtins.int
    RESULT_PER_PAGE_FIELD_NUMBER: builtins.int
    QUERY_FIELD_NUMBER: builtins.int
    NEXT_PAGE_FIELD_NUMBER: builtins.int
    BM25_FIELD_NUMBER: builtins.int
    EMATCHES_FIELD_NUMBER: builtins.int
    fuzzy_distance: builtins.int
    total: builtins.int
    @property
    def results(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___ParagraphResult]:
        """"""
    @property
    def facets(self) -> google.protobuf.internal.containers.MessageMap[builtins.str, global___FacetResults]:
        """For each field what facets are."""
    page_number: builtins.int
    """What page is the answer."""
    result_per_page: builtins.int
    """How many results are in this page."""
    query: builtins.str
    """The text that lead to this results"""
    next_page: builtins.bool
    """Is there a next page"""
    bm25: builtins.bool
    @property
    def ematches(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    def __init__(
        self,
        *,
        fuzzy_distance: builtins.int = ...,
        total: builtins.int = ...,
        results: collections.abc.Iterable[global___ParagraphResult] | None = ...,
        facets: collections.abc.Mapping[builtins.str, global___FacetResults] | None = ...,
        page_number: builtins.int = ...,
        result_per_page: builtins.int = ...,
        query: builtins.str = ...,
        next_page: builtins.bool = ...,
        bm25: builtins.bool = ...,
        ematches: collections.abc.Iterable[builtins.str] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["bm25", b"bm25", "ematches", b"ematches", "facets", b"facets", "fuzzy_distance", b"fuzzy_distance", "next_page", b"next_page", "page_number", b"page_number", "query", b"query", "result_per_page", b"result_per_page", "results", b"results", "total", b"total"]) -> None: ...

global___ParagraphSearchResponse = ParagraphSearchResponse

@typing_extensions.final
class VectorSearchRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    VECTOR_FIELD_NUMBER: builtins.int
    TAGS_FIELD_NUMBER: builtins.int
    PAGE_NUMBER_FIELD_NUMBER: builtins.int
    RESULT_PER_PAGE_FIELD_NUMBER: builtins.int
    WITH_DUPLICATES_FIELD_NUMBER: builtins.int
    RELOAD_FIELD_NUMBER: builtins.int
    id: builtins.str
    """Shard ID"""
    @property
    def vector(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]:
        """Embedded vector search."""
    @property
    def tags(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]:
        """tags to filter"""
    page_number: builtins.int
    """What page is the answer."""
    result_per_page: builtins.int
    """How many results are in this page."""
    with_duplicates: builtins.bool
    reload: builtins.bool
    def __init__(
        self,
        *,
        id: builtins.str = ...,
        vector: collections.abc.Iterable[builtins.float] | None = ...,
        tags: collections.abc.Iterable[builtins.str] | None = ...,
        page_number: builtins.int = ...,
        result_per_page: builtins.int = ...,
        with_duplicates: builtins.bool = ...,
        reload: builtins.bool = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["id", b"id", "page_number", b"page_number", "reload", b"reload", "result_per_page", b"result_per_page", "tags", b"tags", "vector", b"vector", "with_duplicates", b"with_duplicates"]) -> None: ...

global___VectorSearchRequest = VectorSearchRequest

@typing_extensions.final
class DocumentVectorIdentifier(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    id: builtins.str
    def __init__(
        self,
        *,
        id: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["id", b"id"]) -> None: ...

global___DocumentVectorIdentifier = DocumentVectorIdentifier

@typing_extensions.final
class DocumentScored(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DOC_ID_FIELD_NUMBER: builtins.int
    SCORE_FIELD_NUMBER: builtins.int
    @property
    def doc_id(self) -> global___DocumentVectorIdentifier: ...
    score: builtins.float
    def __init__(
        self,
        *,
        doc_id: global___DocumentVectorIdentifier | None = ...,
        score: builtins.float = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["doc_id", b"doc_id"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["doc_id", b"doc_id", "score", b"score"]) -> None: ...

global___DocumentScored = DocumentScored

@typing_extensions.final
class VectorSearchResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DOCUMENTS_FIELD_NUMBER: builtins.int
    PAGE_NUMBER_FIELD_NUMBER: builtins.int
    RESULT_PER_PAGE_FIELD_NUMBER: builtins.int
    @property
    def documents(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___DocumentScored]:
        """List of docs closer to the asked one."""
    page_number: builtins.int
    """What page is the answer."""
    result_per_page: builtins.int
    """How many results are in this page."""
    def __init__(
        self,
        *,
        documents: collections.abc.Iterable[global___DocumentScored] | None = ...,
        page_number: builtins.int = ...,
        result_per_page: builtins.int = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["documents", b"documents", "page_number", b"page_number", "result_per_page", b"result_per_page"]) -> None: ...

global___VectorSearchResponse = VectorSearchResponse

@typing_extensions.final
class RelationFilter(google.protobuf.message.Message):
    """Relation filters are used to make the 
    search domain smaller. By providing filters the 
    search may  be faster.
    """

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NTYPE_FIELD_NUMBER: builtins.int
    SUBTYPE_FIELD_NUMBER: builtins.int
    ntype: nucliadb_protos.utils_pb2.RelationNode.NodeType.ValueType
    """Will filter the search to nodes of type ntype."""
    subtype: builtins.str
    """Additionally the search can be even more specific by 
    providing a subtype. The empty string is a wilcard that 
    indicates to not filter by subtype.
    """
    def __init__(
        self,
        *,
        ntype: nucliadb_protos.utils_pb2.RelationNode.NodeType.ValueType = ...,
        subtype: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["ntype", b"ntype", "subtype", b"subtype"]) -> None: ...

global___RelationFilter = RelationFilter

@typing_extensions.final
class RelationSearchRequest(google.protobuf.message.Message):
    """A request for the relation index."""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    ENTRY_POINTS_FIELD_NUMBER: builtins.int
    TYPE_FILTERS_FIELD_NUMBER: builtins.int
    DEPTH_FIELD_NUMBER: builtins.int
    PREFIX_FIELD_NUMBER: builtins.int
    RELOAD_FIELD_NUMBER: builtins.int
    id: builtins.str
    """Shard ID"""
    @property
    def entry_points(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.utils_pb2.RelationNode]:
        """A search will start from each of the entry points.
        Zero entry points are provided will trigger an iteration
        through all of the nodes.
        """
    @property
    def type_filters(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___RelationFilter]:
        """If needed, the search can be guided through"""
    depth: builtins.int
    """The user can impose a limit in the number of jumps
    the seach may perfom.
    """
    prefix: builtins.str
    """Nodes can be filtered by prefix."""
    reload: builtins.bool
    def __init__(
        self,
        *,
        id: builtins.str = ...,
        entry_points: collections.abc.Iterable[nucliadb_protos.utils_pb2.RelationNode] | None = ...,
        type_filters: collections.abc.Iterable[global___RelationFilter] | None = ...,
        depth: builtins.int = ...,
        prefix: builtins.str = ...,
        reload: builtins.bool = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["depth", b"depth", "entry_points", b"entry_points", "id", b"id", "prefix", b"prefix", "reload", b"reload", "type_filters", b"type_filters"]) -> None: ...

global___RelationSearchRequest = RelationSearchRequest

@typing_extensions.final
class RelationSearchResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    NEIGHBOURS_FIELD_NUMBER: builtins.int
    @property
    def neighbours(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.utils_pb2.RelationNode]: ...
    def __init__(
        self,
        *,
        neighbours: collections.abc.Iterable[nucliadb_protos.utils_pb2.RelationNode] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["neighbours", b"neighbours"]) -> None: ...

global___RelationSearchResponse = RelationSearchResponse

@typing_extensions.final
class SearchRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SHARD_FIELD_NUMBER: builtins.int
    FIELDS_FIELD_NUMBER: builtins.int
    BODY_FIELD_NUMBER: builtins.int
    FILTER_FIELD_NUMBER: builtins.int
    ORDER_FIELD_NUMBER: builtins.int
    FACETED_FIELD_NUMBER: builtins.int
    PAGE_NUMBER_FIELD_NUMBER: builtins.int
    RESULT_PER_PAGE_FIELD_NUMBER: builtins.int
    TIMESTAMPS_FIELD_NUMBER: builtins.int
    VECTOR_FIELD_NUMBER: builtins.int
    RELOAD_FIELD_NUMBER: builtins.int
    PARAGRAPH_FIELD_NUMBER: builtins.int
    DOCUMENT_FIELD_NUMBER: builtins.int
    WITH_DUPLICATES_FIELD_NUMBER: builtins.int
    shard: builtins.str
    @property
    def fields(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    body: builtins.str
    """query this text in all the paragraphs"""
    @property
    def filter(self) -> global___Filter: ...
    @property
    def order(self) -> global___OrderBy: ...
    @property
    def faceted(self) -> global___Faceted:
        """Faceted{ tags: Vec<String>}"""
    page_number: builtins.int
    result_per_page: builtins.int
    @property
    def timestamps(self) -> global___Timestamps: ...
    @property
    def vector(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]:
        """Embedded vector search."""
    reload: builtins.bool
    paragraph: builtins.bool
    document: builtins.bool
    with_duplicates: builtins.bool
    def __init__(
        self,
        *,
        shard: builtins.str = ...,
        fields: collections.abc.Iterable[builtins.str] | None = ...,
        body: builtins.str = ...,
        filter: global___Filter | None = ...,
        order: global___OrderBy | None = ...,
        faceted: global___Faceted | None = ...,
        page_number: builtins.int = ...,
        result_per_page: builtins.int = ...,
        timestamps: global___Timestamps | None = ...,
        vector: collections.abc.Iterable[builtins.float] | None = ...,
        reload: builtins.bool = ...,
        paragraph: builtins.bool = ...,
        document: builtins.bool = ...,
        with_duplicates: builtins.bool = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["faceted", b"faceted", "filter", b"filter", "order", b"order", "timestamps", b"timestamps"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["body", b"body", "document", b"document", "faceted", b"faceted", "fields", b"fields", "filter", b"filter", "order", b"order", "page_number", b"page_number", "paragraph", b"paragraph", "reload", b"reload", "result_per_page", b"result_per_page", "shard", b"shard", "timestamps", b"timestamps", "vector", b"vector", "with_duplicates", b"with_duplicates"]) -> None: ...

global___SearchRequest = SearchRequest

@typing_extensions.final
class SuggestRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SHARD_FIELD_NUMBER: builtins.int
    BODY_FIELD_NUMBER: builtins.int
    FILTER_FIELD_NUMBER: builtins.int
    TIMESTAMPS_FIELD_NUMBER: builtins.int
    shard: builtins.str
    body: builtins.str
    @property
    def filter(self) -> global___Filter: ...
    @property
    def timestamps(self) -> global___Timestamps: ...
    def __init__(
        self,
        *,
        shard: builtins.str = ...,
        body: builtins.str = ...,
        filter: global___Filter | None = ...,
        timestamps: global___Timestamps | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["filter", b"filter", "timestamps", b"timestamps"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["body", b"body", "filter", b"filter", "shard", b"shard", "timestamps", b"timestamps"]) -> None: ...

global___SuggestRequest = SuggestRequest

@typing_extensions.final
class RelatedEntities(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ENTITIES_FIELD_NUMBER: builtins.int
    TOTAL_FIELD_NUMBER: builtins.int
    @property
    def entities(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    total: builtins.int
    def __init__(
        self,
        *,
        entities: collections.abc.Iterable[builtins.str] | None = ...,
        total: builtins.int = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["entities", b"entities", "total", b"total"]) -> None: ...

global___RelatedEntities = RelatedEntities

@typing_extensions.final
class SuggestResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TOTAL_FIELD_NUMBER: builtins.int
    RESULTS_FIELD_NUMBER: builtins.int
    QUERY_FIELD_NUMBER: builtins.int
    EMATCHES_FIELD_NUMBER: builtins.int
    ENTITIES_FIELD_NUMBER: builtins.int
    total: builtins.int
    @property
    def results(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___ParagraphResult]: ...
    query: builtins.str
    """The text that lead to this results"""
    @property
    def ematches(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def entities(self) -> global___RelatedEntities:
        """Entities related with the query"""
    def __init__(
        self,
        *,
        total: builtins.int = ...,
        results: collections.abc.Iterable[global___ParagraphResult] | None = ...,
        query: builtins.str = ...,
        ematches: collections.abc.Iterable[builtins.str] | None = ...,
        entities: global___RelatedEntities | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["entities", b"entities"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["ematches", b"ematches", "entities", b"entities", "query", b"query", "results", b"results", "total", b"total"]) -> None: ...

global___SuggestResponse = SuggestResponse

@typing_extensions.final
class SearchResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DOCUMENT_FIELD_NUMBER: builtins.int
    PARAGRAPH_FIELD_NUMBER: builtins.int
    VECTOR_FIELD_NUMBER: builtins.int
    @property
    def document(self) -> global___DocumentSearchResponse: ...
    @property
    def paragraph(self) -> global___ParagraphSearchResponse: ...
    @property
    def vector(self) -> global___VectorSearchResponse: ...
    def __init__(
        self,
        *,
        document: global___DocumentSearchResponse | None = ...,
        paragraph: global___ParagraphSearchResponse | None = ...,
        vector: global___VectorSearchResponse | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["document", b"document", "paragraph", b"paragraph", "vector", b"vector"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["document", b"document", "paragraph", b"paragraph", "vector", b"vector"]) -> None: ...

global___SearchResponse = SearchResponse

@typing_extensions.final
class IdCollection(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    IDS_FIELD_NUMBER: builtins.int
    @property
    def ids(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    def __init__(
        self,
        *,
        ids: collections.abc.Iterable[builtins.str] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["ids", b"ids"]) -> None: ...

global___IdCollection = IdCollection

@typing_extensions.final
class RelationEdge(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    EDGE_TYPE_FIELD_NUMBER: builtins.int
    PROPERTY_FIELD_NUMBER: builtins.int
    edge_type: nucliadb_protos.utils_pb2.Relation.RelationType.ValueType
    property: builtins.str
    def __init__(
        self,
        *,
        edge_type: nucliadb_protos.utils_pb2.Relation.RelationType.ValueType = ...,
        property: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["edge_type", b"edge_type", "property", b"property"]) -> None: ...

global___RelationEdge = RelationEdge

@typing_extensions.final
class EdgeList(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LIST_FIELD_NUMBER: builtins.int
    @property
    def list(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___RelationEdge]: ...
    def __init__(
        self,
        *,
        list: collections.abc.Iterable[global___RelationEdge] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["list", b"list"]) -> None: ...

global___EdgeList = EdgeList

@typing_extensions.final
class RelationTypeListMember(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    WITH_TYPE_FIELD_NUMBER: builtins.int
    WITH_SUBTYPE_FIELD_NUMBER: builtins.int
    with_type: nucliadb_protos.utils_pb2.RelationNode.NodeType.ValueType
    with_subtype: builtins.str
    def __init__(
        self,
        *,
        with_type: nucliadb_protos.utils_pb2.RelationNode.NodeType.ValueType = ...,
        with_subtype: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["with_subtype", b"with_subtype", "with_type", b"with_type"]) -> None: ...

global___RelationTypeListMember = RelationTypeListMember

@typing_extensions.final
class TypeList(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LIST_FIELD_NUMBER: builtins.int
    @property
    def list(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___RelationTypeListMember]: ...
    def __init__(
        self,
        *,
        list: collections.abc.Iterable[global___RelationTypeListMember] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["list", b"list"]) -> None: ...

global___TypeList = TypeList
