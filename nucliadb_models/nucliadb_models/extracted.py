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
from typing import Dict, List, Optional, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel

from nucliadb_protos import resources_pb2

from .common import Classification, CloudFile, CloudLink, FieldID, Paragraph

_T = TypeVar("_T")


class ExtractedText(BaseModel):
    text: Optional[str]
    split_text: Optional[Dict[str, str]]
    deleted_splits: Optional[List[str]]

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
    body: Optional[ExtractedText]
    file: Optional[CloudFile]
    field: Optional[FieldID]


class Vector(BaseModel):
    start: Optional[int]
    end: Optional[int]
    start_paragraph: Optional[int]
    end_paragraph: Optional[int]
    vector: Optional[List[float]]


class Vectors(BaseModel):
    vectors: Optional[List[Vector]]


class VectorObject(BaseModel):
    vectors: Optional[Vectors]
    split_vectors: Optional[Dict[str, Vectors]]
    deleted_splits: Optional[List[str]]

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
    vectors: Optional[VectorObject]
    file: Optional[CloudFile]
    field: Optional[FieldID]


class FieldMetadata(BaseModel):
    links: List[str]
    paragraphs: List[Paragraph]
    ner: Dict[str, str]
    classifications: List[Classification]
    last_index: Optional[datetime]
    last_understanding: Optional[datetime]
    last_extract: Optional[datetime]
    last_summary: Optional[datetime]
    thumbnail: Optional[CloudLink]
    language: Optional[str]
    summary: Optional[str]


class FieldComputedMetadata(BaseModel):
    metadata: FieldMetadata
    split_metadata: Optional[Dict[str, FieldMetadata]]
    deleted_splits: Optional[List[str]]

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.FieldComputedMetadata) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


class FieldComputedMetadataWrapper(BaseModel):
    metadata: Optional[FieldComputedMetadata]
    field: Optional[FieldID]


class Entity(BaseModel):
    token: Optional[str]
    root: Optional[str]
    type: Optional[str]


class FieldLargeMetadata(BaseModel):
    entities: Optional[List[Entity]]
    tokens: Optional[Dict[str, int]]


class LargeComputedMetadata(BaseModel):
    metadata: Optional[FieldLargeMetadata]
    split_metadata: Optional[Dict[str, FieldLargeMetadata]]
    deleted_splits: Optional[List[str]]

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
    real: Optional[LargeComputedMetadata]
    file: Optional[CloudFile]
    field: Optional[FieldID]


class LinkExtractedData(BaseModel):
    date: Optional[datetime]
    language: Optional[str]
    title: Optional[str]
    metadata: Optional[Dict[str, str]]
    link_thumbnail: Optional[CloudLink]
    link_preview: Optional[CloudLink]
    field: Optional[str]
    link_image: Optional[CloudLink]
    description: Optional[str]
    type: Optional[str]
    embed: Optional[str]

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
    start: Optional[int]
    end: Optional[int]
    page: Optional[int]


class Row(BaseModel):
    cell: Optional[List[str]]


class Sheet(BaseModel):
    rows: Optional[List[Row]]


class RowsPreview(BaseModel):
    sheets: Optional[Dict[str, Sheet]]


class PagePositions(BaseModel):
    start: Optional[int]
    end: Optional[int]


class FilePages(BaseModel):
    pages: Optional[List[CloudLink]]
    positions: Optional[List[PagePositions]]


class FileExtractedData(BaseModel):
    language: Optional[str]
    md5: Optional[str]
    metadata: Optional[Dict[str, str]]
    nested: Optional[Dict[str, str]]
    file_generated: Optional[Dict[str, CloudLink]]
    file_rows_previews: Optional[Dict[str, RowsPreview]]
    file_preview: Optional[CloudLink]
    file_pages_previews: Optional[FilePages]
    file_thumbnail: Optional[CloudLink]
    field: Optional[str]
    icon: Optional[str]
    nested_position: Optional[Dict[str, NestedPosition]]

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.FileExtractedData) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )
