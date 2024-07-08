# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.2.6.2](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.3
# Pydantic Version: 1.10.14
import typing
from enum import IntEnum

from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel, Field

from .nodewriter_p2p import VectorIndexConfig
from .utils_p2p import ReleaseChannel, VectorSimilarity


class KnowledgeBoxResponseStatus(IntEnum):
    OK = 0
    CONFLICT = 1
    NOTFOUND = 2
    ERROR = 3


class KnowledgeBoxID(BaseModel):
    slug: str = Field(default="")
    uuid: str = Field(default="")


class KnowledgeBoxConfig(BaseModel):
    title: str = Field(default="")
    description: str = Field(default="")
    slug: str = Field(default="")
    migration_version: int = Field(default=0)
    enabled_filters: typing.List[str] = Field(default_factory=list)
    enabled_insights: typing.List[str] = Field(default_factory=list)
    disable_vectors: bool = Field(default=False)
    release_channel: ReleaseChannel = Field(default=0)


class KnowledgeBoxNew(BaseModel):
    slug: str = Field(default="")
    config: KnowledgeBoxConfig = Field()
    forceuuid: str = Field(default="")
    similarity: VectorSimilarity = Field(default=0)
    vector_dimension: typing.Optional[int] = Field(default=0)
    default_min_score: typing.Optional[float] = Field(default=0.0)
    matryoshka_dimensions: typing.List[int] = Field(default_factory=list)
    learning_config: str = Field(default="")
    release_channel: ReleaseChannel = Field(default=0)


class NewKnowledgeBoxResponse(BaseModel):
    status: KnowledgeBoxResponseStatus = Field(default=0)
    uuid: str = Field(default="")


class KnowledgeBoxUpdate(BaseModel):
    slug: str = Field(default="")
    uuid: str = Field(default="")
    config: KnowledgeBoxConfig = Field()


class UpdateKnowledgeBoxResponse(BaseModel):
    status: KnowledgeBoxResponseStatus = Field(default=0)
    uuid: str = Field(default="")


class DeleteKnowledgeBoxResponse(BaseModel):
    status: KnowledgeBoxResponseStatus = Field(default=0)


class Label(BaseModel):
    title: str = Field(default="")
    related: str = Field(default="")
    text: str = Field(default="")
    uri: str = Field(default="")


class LabelSet(BaseModel):
    class LabelSetKind(IntEnum):
        RESOURCES = 0
        PARAGRAPHS = 1
        SENTENCES = 2
        SELECTIONS = 3

    title: str = Field(default="")
    color: str = Field(default="")
    labels: typing.List[Label] = Field(default_factory=list)
    multiple: bool = Field(default=False)
    kind: typing.List["LabelSet.LabelSetKind"] = Field(default_factory=list)


class Labels(BaseModel):
    labelset: typing.Dict[str, LabelSet] = Field(default_factory=dict)


class Entity(BaseModel):
    value: str = Field(default="")
    represents: typing.List[str] = Field(default_factory=list)
    merged: bool = Field(default=False)
    deleted: bool = Field(default=False)


class EntitiesGroupSummary(BaseModel):
    title: str = Field(default="")
    color: str = Field(default="")
    custom: bool = Field(default=False)


class EntitiesGroup(BaseModel):
    entities: typing.Dict[str, Entity] = Field(default_factory=dict)
    title: str = Field(default="")
    color: str = Field(default="")
    custom: bool = Field(default=False)


class DeletedEntitiesGroups(BaseModel):
    entities_groups: typing.List[str] = Field(default_factory=list)


class EntitiesGroups(BaseModel):
    entities_groups: typing.Dict[str, EntitiesGroup] = Field(default_factory=dict)


class EntityGroupDuplicateIndex(BaseModel):
    class EntityDuplicates(BaseModel):
        duplicates: typing.List[str] = Field(default_factory=list)

    class EntityGroupDuplicates(BaseModel):
        entities: typing.Dict[str, EntityDuplicates] = Field(default_factory=dict)

    entities_groups: typing.Dict[str, EntityGroupDuplicates] = Field(
        default_factory=dict
    )


class VectorSet(BaseModel):
    dimension: int = Field(default=0)
    similarity: VectorSimilarity = Field(default=0)


class VectorSets(BaseModel):
    vectorsets: typing.Dict[str, VectorSet] = Field(default_factory=dict)


class VectorSetConfig(BaseModel):
    vectorset_id: str = Field(default="")
    vectorset_index_config: VectorIndexConfig = Field()
    matryoshka_dimensions: typing.List[int] = Field(default_factory=list)


class KnowledgeBoxVectorSetsConfig(BaseModel):
    vectorsets: typing.List[VectorSetConfig] = Field(default_factory=list)


class TermSynonyms(BaseModel):
    synonyms: typing.List[str] = Field(default_factory=list)


class Synonyms(BaseModel):
    terms: typing.Dict[str, TermSynonyms] = Field(default_factory=dict)


class SemanticModelMetadata(BaseModel):
    similarity_function: VectorSimilarity = Field(default=0)
    vector_dimension: typing.Optional[int] = Field(default=0)
    default_min_score: typing.Optional[float] = Field(default=0.0)
    matryoshka_dimensions: typing.List[int] = Field(default_factory=list)


class KBConfiguration(BaseModel):
    semantic_model: str = Field(default="")
    generative_model: str = Field(default="")
    ner_model: str = Field(default="")
    anonymization_model: str = Field(default="")
    visual_labeling: str = Field(default="")
