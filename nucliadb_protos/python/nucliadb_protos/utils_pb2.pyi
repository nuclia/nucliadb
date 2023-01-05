"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import typing
import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class Relation(google.protobuf.message.Message):
    """Relations are connexions between nodes in the relation index. 
    They are tuplets (Source, Relation Type, Relation Label, To).
    """
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class _RelationType:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _RelationTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[Relation._RelationType.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        CHILD: Relation._RelationType.ValueType  # 0
        """Child resource"""

        ABOUT: Relation._RelationType.ValueType  # 1
        """related with label (GENERATED)"""

        ENTITY: Relation._RelationType.ValueType  # 2
        """related with an entity (GENERATED)"""

        COLAB: Relation._RelationType.ValueType  # 3
        """related with user (GENERATED)"""

        SYNONYM: Relation._RelationType.ValueType  # 4
        """Synonym relation"""

        OTHER: Relation._RelationType.ValueType  # 5
        """related with something"""

    class RelationType(_RelationType, metaclass=_RelationTypeEnumTypeWrapper):
        pass

    CHILD: Relation.RelationType.ValueType  # 0
    """Child resource"""

    ABOUT: Relation.RelationType.ValueType  # 1
    """related with label (GENERATED)"""

    ENTITY: Relation.RelationType.ValueType  # 2
    """related with an entity (GENERATED)"""

    COLAB: Relation.RelationType.ValueType  # 3
    """related with user (GENERATED)"""

    SYNONYM: Relation.RelationType.ValueType  # 4
    """Synonym relation"""

    OTHER: Relation.RelationType.ValueType  # 5
    """related with something"""


    RELATION_FIELD_NUMBER: builtins.int
    SOURCE_FIELD_NUMBER: builtins.int
    TO_FIELD_NUMBER: builtins.int
    RELATION_LABEL_FIELD_NUMBER: builtins.int
    relation: global___Relation.RelationType.ValueType
    """relation is the type of the label."""

    @property
    def source(self) -> global___RelationNode:
        """The source of the edge."""
        pass
    @property
    def to(self) -> global___RelationNode:
        """The destination of the edge."""
        pass
    relation_label: typing.Text
    """Apart of having types, edges may be valued like
    in the case of 'OTHER' edges.
    """

    def __init__(self,
        *,
        relation: global___Relation.RelationType.ValueType = ...,
        source: typing.Optional[global___RelationNode] = ...,
        to: typing.Optional[global___RelationNode] = ...,
        relation_label: typing.Text = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["source",b"source","to",b"to"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["relation",b"relation","relation_label",b"relation_label","source",b"source","to",b"to"]) -> None: ...
global___Relation = Relation

class RelationNode(google.protobuf.message.Message):
    """Nodes are tuplets (Value, Type, Subtype) and they are the main element in the relation index."""
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class _NodeType:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _NodeTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[RelationNode._NodeType.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        ENTITY: RelationNode._NodeType.ValueType  # 0
        LABEL: RelationNode._NodeType.ValueType  # 1
        RESOURCE: RelationNode._NodeType.ValueType  # 2
        USER: RelationNode._NodeType.ValueType  # 3
    class NodeType(_NodeType, metaclass=_NodeTypeEnumTypeWrapper):
        pass

    ENTITY: RelationNode.NodeType.ValueType  # 0
    LABEL: RelationNode.NodeType.ValueType  # 1
    RESOURCE: RelationNode.NodeType.ValueType  # 2
    USER: RelationNode.NodeType.ValueType  # 3

    VALUE_FIELD_NUMBER: builtins.int
    NTYPE_FIELD_NUMBER: builtins.int
    SUBTYPE_FIELD_NUMBER: builtins.int
    value: typing.Text
    """Value of the node."""

    ntype: global___RelationNode.NodeType.ValueType
    """The type of the node."""

    subtype: typing.Text
    """A node may have a subtype (the string should be empty in case it does not)."""

    def __init__(self,
        *,
        value: typing.Text = ...,
        ntype: global___RelationNode.NodeType.ValueType = ...,
        subtype: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["ntype",b"ntype","subtype",b"subtype","value",b"value"]) -> None: ...
global___RelationNode = RelationNode

class JoinGraphCnx(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SOURCE_FIELD_NUMBER: builtins.int
    TARGET_FIELD_NUMBER: builtins.int
    RTYPE_FIELD_NUMBER: builtins.int
    RSUBTYPE_FIELD_NUMBER: builtins.int
    source: builtins.int
    target: builtins.int
    rtype: global___Relation.RelationType.ValueType
    rsubtype: typing.Text
    def __init__(self,
        *,
        source: builtins.int = ...,
        target: builtins.int = ...,
        rtype: global___Relation.RelationType.ValueType = ...,
        rsubtype: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["rsubtype",b"rsubtype","rtype",b"rtype","source",b"source","target",b"target"]) -> None: ...
global___JoinGraphCnx = JoinGraphCnx

class JoinGraph(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class NodesEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: builtins.int
        @property
        def value(self) -> global___RelationNode: ...
        def __init__(self,
            *,
            key: builtins.int = ...,
            value: typing.Optional[global___RelationNode] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    NODES_FIELD_NUMBER: builtins.int
    EDGES_FIELD_NUMBER: builtins.int
    @property
    def nodes(self) -> google.protobuf.internal.containers.MessageMap[builtins.int, global___RelationNode]: ...
    @property
    def edges(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___JoinGraphCnx]: ...
    def __init__(self,
        *,
        nodes: typing.Optional[typing.Mapping[builtins.int, global___RelationNode]] = ...,
        edges: typing.Optional[typing.Iterable[global___JoinGraphCnx]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["edges",b"edges","nodes",b"nodes"]) -> None: ...
global___JoinGraph = JoinGraph

class ExtractedText(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class SplitTextEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        value: typing.Text
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Text = ...,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    TEXT_FIELD_NUMBER: builtins.int
    SPLIT_TEXT_FIELD_NUMBER: builtins.int
    DELETED_SPLITS_FIELD_NUMBER: builtins.int
    text: typing.Text
    @property
    def split_text(self) -> google.protobuf.internal.containers.ScalarMap[typing.Text, typing.Text]: ...
    @property
    def deleted_splits(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    def __init__(self,
        *,
        text: typing.Text = ...,
        split_text: typing.Optional[typing.Mapping[typing.Text, typing.Text]] = ...,
        deleted_splits: typing.Optional[typing.Iterable[typing.Text]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["deleted_splits",b"deleted_splits","split_text",b"split_text","text",b"text"]) -> None: ...
global___ExtractedText = ExtractedText

class Vector(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    START_FIELD_NUMBER: builtins.int
    END_FIELD_NUMBER: builtins.int
    START_PARAGRAPH_FIELD_NUMBER: builtins.int
    END_PARAGRAPH_FIELD_NUMBER: builtins.int
    VECTOR_FIELD_NUMBER: builtins.int
    start: builtins.int
    end: builtins.int
    start_paragraph: builtins.int
    end_paragraph: builtins.int
    @property
    def vector(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
    def __init__(self,
        *,
        start: builtins.int = ...,
        end: builtins.int = ...,
        start_paragraph: builtins.int = ...,
        end_paragraph: builtins.int = ...,
        vector: typing.Optional[typing.Iterable[builtins.float]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["end",b"end","end_paragraph",b"end_paragraph","start",b"start","start_paragraph",b"start_paragraph","vector",b"vector"]) -> None: ...
global___Vector = Vector

class Vectors(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    VECTORS_FIELD_NUMBER: builtins.int
    @property
    def vectors(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Vector]: ...
    def __init__(self,
        *,
        vectors: typing.Optional[typing.Iterable[global___Vector]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["vectors",b"vectors"]) -> None: ...
global___Vectors = Vectors

class VectorObject(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class SplitVectorsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> global___Vectors: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[global___Vectors] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    VECTORS_FIELD_NUMBER: builtins.int
    SPLIT_VECTORS_FIELD_NUMBER: builtins.int
    DELETED_SPLITS_FIELD_NUMBER: builtins.int
    @property
    def vectors(self) -> global___Vectors: ...
    @property
    def split_vectors(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, global___Vectors]: ...
    @property
    def deleted_splits(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    def __init__(self,
        *,
        vectors: typing.Optional[global___Vectors] = ...,
        split_vectors: typing.Optional[typing.Mapping[typing.Text, global___Vectors]] = ...,
        deleted_splits: typing.Optional[typing.Iterable[typing.Text]] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["vectors",b"vectors"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["deleted_splits",b"deleted_splits","split_vectors",b"split_vectors","vectors",b"vectors"]) -> None: ...
global___VectorObject = VectorObject

class UserVector(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    VECTOR_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    START_FIELD_NUMBER: builtins.int
    END_FIELD_NUMBER: builtins.int
    @property
    def vector(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
    @property
    def labels(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    start: builtins.int
    end: builtins.int
    def __init__(self,
        *,
        vector: typing.Optional[typing.Iterable[builtins.float]] = ...,
        labels: typing.Optional[typing.Iterable[typing.Text]] = ...,
        start: builtins.int = ...,
        end: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["end",b"end","labels",b"labels","start",b"start","vector",b"vector"]) -> None: ...
global___UserVector = UserVector

class UserVectors(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class VectorsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> global___UserVector: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[global___UserVector] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    VECTORS_FIELD_NUMBER: builtins.int
    @property
    def vectors(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, global___UserVector]:
        """vector's id"""
        pass
    def __init__(self,
        *,
        vectors: typing.Optional[typing.Mapping[typing.Text, global___UserVector]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["vectors",b"vectors"]) -> None: ...
global___UserVectors = UserVectors

class UserVectorSet(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class VectorsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> global___UserVectors: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[global___UserVectors] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    VECTORS_FIELD_NUMBER: builtins.int
    @property
    def vectors(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, global___UserVectors]:
        """vectorsets"""
        pass
    def __init__(self,
        *,
        vectors: typing.Optional[typing.Mapping[typing.Text, global___UserVectors]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["vectors",b"vectors"]) -> None: ...
global___UserVectorSet = UserVectorSet

class UserVectorsList(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    VECTORS_FIELD_NUMBER: builtins.int
    @property
    def vectors(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    def __init__(self,
        *,
        vectors: typing.Optional[typing.Iterable[typing.Text]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["vectors",b"vectors"]) -> None: ...
global___UserVectorsList = UserVectorsList
