# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from datetime import datetime

from pydantic import BaseModel, Field

from .common import (
    Classification,
    CloudLink,
    Paragraph,
    QuestionAnswers,
)
from .metadata import Relation


class ExtractedText(BaseModel):
    text: str | None = None
    split_text: dict[str, str] | None = None
    deleted_splits: list[str] | None = None


class Vector(BaseModel):
    start: int | None = None
    end: int | None = None
    start_paragraph: int | None = None
    end_paragraph: int | None = None
    vector: list[float] | None = None


class Vectors(BaseModel):
    vectors: list[Vector] | None = None


class VectorObject(BaseModel):
    vectors: Vectors | None = None
    split_vectors: dict[str, Vectors] | None = None
    deleted_splits: list[str] | None = None


class Position(BaseModel):
    start: int
    end: int


class Positions(BaseModel):
    position: list[Position]
    entity: str


class FieldEntity(BaseModel):
    text: str
    label: str
    positions: list[Position]


class FieldEntities(BaseModel):
    """
    Wrapper for the entities extracted from a field (required because protobuf doesn't support lists of lists)
    """

    entities: list[FieldEntity]


class FieldMetadata(BaseModel):
    links: list[str]
    paragraphs: list[Paragraph]
    ner: dict[str, str]  # TODO: Remove once processor doesn't use this anymore
    entities: dict[str, FieldEntities]
    classifications: list[Classification]
    last_index: datetime | None = None
    last_understanding: datetime | None = None
    last_extract: datetime | None = None
    last_summary: datetime | None = None
    last_processing_start: datetime | None = None
    thumbnail: CloudLink | None = None
    language: str | None = None
    summary: str | None = None
    positions: dict[str, Positions]  # TODO: Remove once processor doesn't use this anymore
    relations: list[Relation] | None = None
    mime_type: str | None = None


class FieldComputedMetadata(BaseModel):
    metadata: FieldMetadata
    split_metadata: dict[str, FieldMetadata] | None = None
    deleted_splits: list[str] | None = None


class Entity(BaseModel):
    token: str | None = None
    root: str | None = None
    type: str | None = None


class FieldLargeMetadata(BaseModel):
    entities: list[Entity] | None = None
    tokens: dict[str, int] | None = None


class LargeComputedMetadata(BaseModel):
    metadata: FieldLargeMetadata | None = None
    split_metadata: dict[str, FieldLargeMetadata] | None = None
    deleted_splits: list[str] | None = None


class LinkExtractedData(BaseModel):
    date: datetime | None = None
    language: str | None = None
    title: str | None = None
    metadata: dict[str, str] | None = None
    link_thumbnail: CloudLink | None = None
    link_preview: CloudLink | None = None
    field: str | None = Field(default=None, title="Link Extracted Data Field")
    link_image: CloudLink | None = None
    description: str | None = None
    type: str | None = None
    embed: str | None = None
    file_generated: dict[str, CloudLink] | None = None


class NestedPosition(BaseModel):
    start: int | None = None
    end: int | None = None
    page: int | None = Field(default=None, title="Position Page")


class NestedListPosition(BaseModel):
    positions: list[NestedPosition]


class Row(BaseModel):
    cell: list[str] | None = None


class Sheet(BaseModel):
    rows: list[Row] | None = None


class RowsPreview(BaseModel):
    sheets: dict[str, Sheet] | None = None


class PagePositions(BaseModel):
    start: int | None = None
    end: int | None = None


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
    tokens: list[PageStructureToken]


class FilePages(BaseModel):
    pages: list[CloudLink] | None = None
    positions: list[PagePositions] | None = None
    structures: list[PageStructure] | None = None


class FileExtractedData(BaseModel):
    language: str | None = None
    md5: str | None = None
    metadata: dict[str, str] | None = None
    nested: dict[str, str] | None = None
    file_generated: dict[str, CloudLink] | None = None
    file_rows_previews: dict[str, RowsPreview] | None = None
    file_preview: CloudLink | None = None
    file_pages_previews: FilePages | None = None
    file_thumbnail: CloudLink | None = None
    field: str | None = None
    icon: str | None = None
    nested_position: dict[str, NestedPosition] | None = None
    nested_list_position: dict[str, NestedListPosition] | None = None


class FieldQuestionAnswers(BaseModel):
    question_answers: QuestionAnswers
    split_question_answers: dict[str, QuestionAnswers] | None = None
    deleted_splits: list[str] | None = None
