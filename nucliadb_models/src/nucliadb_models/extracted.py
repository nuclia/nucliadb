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
from typing import Dict, List, Optional

from pydantic import BaseModel

from .common import (
    Classification,
    CloudLink,
    Paragraph,
    QuestionAnswers,
)
from .metadata import Relation


class ExtractedText(BaseModel):
    text: Optional[str] = None
    split_text: Optional[Dict[str, str]] = None
    deleted_splits: Optional[List[str]] = None


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


class Position(BaseModel):
    start: int
    end: int


class Positions(BaseModel):
    position: List[Position]
    entity: str


class FieldEntity(BaseModel):
    text: str
    label: str
    positions: List[Position]


class FieldEntities(BaseModel):
    """
    Wrapper for the entities extracted from a field (required because protobuf doesn't support lists of lists)
    """

    entities: List[FieldEntity]


class FieldMetadata(BaseModel):
    links: List[str]
    paragraphs: List[Paragraph]
    ner: Dict[str, str]  # TODO: Remove once processor doesn't use this anymore
    entities: Dict[str, FieldEntities]
    classifications: List[Classification]
    last_index: Optional[datetime] = None
    last_understanding: Optional[datetime] = None
    last_extract: Optional[datetime] = None
    last_summary: Optional[datetime] = None
    last_processing_start: Optional[datetime] = None
    thumbnail: Optional[CloudLink] = None
    language: Optional[str] = None
    summary: Optional[str] = None
    positions: Dict[str, Positions]  # TODO: Remove once processor doesn't use this anymore
    relations: Optional[List[Relation]] = None
    mime_type: Optional[str] = None


class FieldComputedMetadata(BaseModel):
    metadata: FieldMetadata
    split_metadata: Optional[Dict[str, FieldMetadata]] = None
    deleted_splits: Optional[List[str]] = None


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


class FieldQuestionAnswers(BaseModel):
    question_answers: QuestionAnswers
    split_question_answers: Optional[Dict[str, QuestionAnswers]] = None
    deleted_splits: Optional[List[str]] = None
