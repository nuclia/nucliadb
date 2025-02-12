# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.0.3](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.5 
# Pydantic Version: 2.10.4 
from enum import IntEnum
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import Field
import typing

class VectorSimilarity(IntEnum):
    COSINE = 0
    DOT = 1


class ReleaseChannel(IntEnum):
    STABLE = 0
    EXPERIMENTAL = 1

class RelationNode(BaseModel):
    """
     Nodes are tuplets (Value, Type, Subtype) and they are the main element in the relation index.
    """
    class NodeType(IntEnum):
        ENTITY = 0
        LABEL = 1
        RESOURCE = 2
        USER = 3

# Value of the node.
    value: str = Field(default="")
# The type of the node.
    ntype: NodeType = Field(default=0)
# A node may have a subtype (the string should be empty in case it does not).
    subtype: str = Field(default="")

class RelationMetadata(BaseModel):
    paragraph_id: typing.Optional[str] = Field(default="")
    source_start: typing.Optional[int] = Field(default=0)
    source_end: typing.Optional[int] = Field(default=0)
    to_start: typing.Optional[int] = Field(default=0)
    to_end: typing.Optional[int] = Field(default=0)
    data_augmentation_task_id: typing.Optional[str] = Field(default="")# Data augmentation task id where this relation was computed

class Relation(BaseModel):
    """
     Relations are connexions between nodes in the relation index.
 They are tuplets (Source, Relation Type, Relation Label, To).
    """
    class RelationType(IntEnum):
        CHILD = 0
        ABOUT = 1
        ENTITY = 2
        COLAB = 3
        SYNONYM = 4
        OTHER = 5

    source: RelationNode = Field()
    to: RelationNode = Field()
    relation: "Relation.RelationType" = Field(default=0)
    relation_label: str = Field(default="")
    metadata: RelationMetadata = Field()

class ExtractedText(BaseModel):
    text: str = Field(default="")
    split_text: typing.Dict[str, str] = Field(default_factory=dict)
    deleted_splits: typing.List[str] = Field(default_factory=list)

class Vector(BaseModel):
    start: int = Field(default=0)
    end: int = Field(default=0)
    start_paragraph: int = Field(default=0)
    end_paragraph: int = Field(default=0)
    vector: typing.List[float] = Field(default_factory=list)

class Vectors(BaseModel):
    vectors: typing.List[Vector] = Field(default_factory=list)

class VectorObject(BaseModel):
    vectors: Vectors = Field()
    split_vectors: typing.Dict[str, Vectors] = Field(default_factory=dict)
    deleted_splits: typing.List[str] = Field(default_factory=list)

class UserVector(BaseModel):
    vector: typing.List[float] = Field(default_factory=list)
    labels: typing.List[str] = Field(default_factory=list)
    start: int = Field(default=0)
    end: int = Field(default=0)

class UserVectors(BaseModel):
    vectors: typing.Dict[str, UserVector] = Field(default_factory=dict)# vector's id

class UserVectorSet(BaseModel):
    vectors: typing.Dict[str, UserVectors] = Field(default_factory=dict)# vectorsets

class UserVectorsList(BaseModel):
    vectors: typing.List[str] = Field(default_factory=list)

class Security(BaseModel):
    access_groups: typing.List[str] = Field(default_factory=list)
