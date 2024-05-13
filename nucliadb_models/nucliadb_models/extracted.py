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
from typing import Any, Dict, List, Optional, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel

from nucliadb_protos import resources_pb2

from .common import (
    Classification,
    CloudFile,
    CloudLink,
    FieldID,
    Paragraph,
    QuestionAnswer,
)
from .metadata import Relation, convert_pb_relation_to_api

_T = TypeVar("_T")


class ExtractedText(BaseModel):
    text: Optional[str] = None
    split_text: Optional[Dict[str, str]] = None
    deleted_splits: Optional[List[str]] = None

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.ExtractedText) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


class ExtractedTextWrapper(BaseModel):
    body: Optional[ExtractedText] = None
    file: Optional[CloudFile] = None
    field: Optional[FieldID] = None


class Vector(BaseModel):
    start: Optional[int] = None
    end: Optional[int] = None
    start_paragraph: Optional[int] = None
    end_paragraph: Optional[int] = None
    vector: Optional[List[float]] = None


class Vectors(BaseModel):
    vectors: Optional[List[Vector]] = None


class VectorObject(BaseModel):
    vectors: Optional[Vectors] = None
    split_vectors: Optional[Dict[str, Vectors]] = None
    deleted_splits: Optional[List[str]] = None

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.VectorObject) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


class ExtractedVectorsWrapper(BaseModel):
    vectors: Optional[VectorObject] = None
    file: Optional[CloudFile] = None
    field: Optional[FieldID] = None


class Position(BaseModel):
    start: int
    end: int


class Positions(BaseModel):
    position: List[Position]
    entity: str


class FieldMetadata(BaseModel):
    links: List[str]
    paragraphs: List[Paragraph]
    ner: Dict[str, str]
    classifications: List[Classification]
    last_index: Optional[datetime] = None
    last_understanding: Optional[datetime] = None
    last_extract: Optional[datetime] = None
    last_summary: Optional[datetime] = None
    thumbnail: Optional[CloudLink] = None
    language: Optional[str] = None
    summary: Optional[str] = None
    positions: Dict[str, Positions]
    relations: Optional[List[Relation]] = None


class FieldComputedMetadata(BaseModel):
    metadata: FieldMetadata
    split_metadata: Optional[Dict[str, FieldMetadata]] = None
    deleted_splits: Optional[List[str]] = None

    @classmethod
    def from_message(
        cls,
        message: resources_pb2.FieldComputedMetadata,
        shortened: bool = False,
    ):
        if shortened:
            cls.shorten_fieldmetadata(message)
        metadata = convert_fieldmetadata_pb_to_dict(message.metadata)
        split_metadata = {
            split: convert_fieldmetadata_pb_to_dict(metadata_split)
            for split, metadata_split in message.split_metadata.items()
        }
        value = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        value["metadata"] = metadata
        value["split_metadata"] = split_metadata
        return cls(**value)

    @classmethod
    def shorten_fieldmetadata(
        cls,
        message: resources_pb2.FieldComputedMetadata,
    ) -> None:
        large_fields = ["ner", "relations", "positions", "classifications"]
        for field in large_fields:
            message.metadata.ClearField(field)  # type: ignore
        for metadata in message.split_metadata.values():
            for field in large_fields:
                metadata.ClearField(field)  # type: ignore


class FieldComputedMetadataWrapper(BaseModel):
    metadata: Optional[FieldComputedMetadata] = None
    field: Optional[FieldID] = None


class Entity(BaseModel):
    token: Optional[str] = None
    root: Optional[str] = None
    type: Optional[str] = None


class FieldLargeMetadata(BaseModel):
    entities: Optional[List[Entity]] = None
    tokens: Optional[Dict[str, int]] = None


class LargeComputedMetadata(BaseModel):
    metadata: Optional[FieldLargeMetadata] = None
    split_metadata: Optional[Dict[str, FieldLargeMetadata]] = None
    deleted_splits: Optional[List[str]] = None

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.LargeComputedMetadata) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


class LargeComputedMetadataWrapper(BaseModel):
    real: Optional[LargeComputedMetadata] = None
    file: Optional[CloudFile] = None
    field: Optional[FieldID] = None


class LinkExtractedData(BaseModel):
    date: Optional[datetime] = None
    language: Optional[str] = None
    title: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    link_thumbnail: Optional[CloudLink] = None
    link_preview: Optional[CloudLink] = None
    field: Optional[str] = None
    link_image: Optional[CloudLink] = None
    description: Optional[str] = None
    type: Optional[str] = None
    embed: Optional[str] = None
    file_generated: Optional[Dict[str, CloudLink]] = None

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.LinkExtractedData) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


class NestedPosition(BaseModel):
    start: Optional[int] = None
    end: Optional[int] = None
    page: Optional[int] = None


class NestedListPosition(BaseModel):
    positions: List[NestedPosition]


class Row(BaseModel):
    cell: Optional[List[str]] = None


class Sheet(BaseModel):
    rows: Optional[List[Row]] = None


class RowsPreview(BaseModel):
    sheets: Optional[Dict[str, Sheet]] = None


class PagePositions(BaseModel):
    start: Optional[int] = None
    end: Optional[int] = None


class PageStructurePage(BaseModel):
    width: int
    height: int


class PageStructureToken(BaseModel):
    x: float
    y: float
    width: float
    height: float
    text: str
    line: float


class PageStructure(BaseModel):
    page: PageStructurePage
    tokens: List[PageStructureToken]


class FilePages(BaseModel):
    pages: Optional[List[CloudLink]] = None
    positions: Optional[List[PagePositions]] = None
    structures: Optional[List[PageStructure]] = None


class FileExtractedData(BaseModel):
    language: Optional[str] = None
    md5: Optional[str] = None
    metadata: Optional[Dict[str, str]] = None
    nested: Optional[Dict[str, str]] = None
    file_generated: Optional[Dict[str, CloudLink]] = None
    file_rows_previews: Optional[Dict[str, RowsPreview]] = None
    file_preview: Optional[CloudLink] = None
    file_pages_previews: Optional[FilePages] = None
    file_thumbnail: Optional[CloudLink] = None
    field: Optional[str] = None
    icon: Optional[str] = None
    nested_position: Optional[Dict[str, NestedPosition]] = None
    nested_list_position: Optional[Dict[str, NestedListPosition]] = None

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.FileExtractedData) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


class QuestionAnswers(BaseModel):
    question_answer: List[QuestionAnswer]

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.QuestionAnswers) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


def convert_fieldmetadata_pb_to_dict(
    message: resources_pb2.FieldMetadata,
) -> Dict[str, Any]:
    value = MessageToDict(
        message,
        preserving_proto_field_name=True,
        including_default_value_fields=True,
    )
    value["relations"] = [
        convert_pb_relation_to_api(relation)
        for relations in message.relations
        for relation in relations.relations
    ]
    return value
