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

from enum import Enum
from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, model_validator
from typing_extensions import Self

from nucliadb_models import filters
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import Image, ResourceProperties

ResourceIdPattern = r"^[0-9a-f]{32}$"
ResourceId = Annotated[
    str,
    StringConstraints(pattern=ResourceIdPattern, min_length=32, max_length=32),
]

FieldIdPattern = r"^[0-9a-f]{32}/[acftu]/[a-zA-Z0-9:_-]+(/[^/]{1,128})?$"
FieldId = Annotated[
    str,
    StringConstraints(
        pattern=FieldIdPattern,
        min_length=32 + 1 + 1 + 1 + 1 + 0 + 0,
        # max field id of 250
        max_length=32 + 1 + 1 + 1 + 250 + 1 + 218,
    ),
]

ParagraphIdPattern = r"^[0-9a-f]{32}/[acftu]/[a-zA-Z0-9:_-]+(/[^/]{1,128})?/[0-9]+-[0-9]+$"
ParagraphId = Annotated[
    str,
    StringConstraints(
        # resource-uuid/field-type/field-id/[split-id/]paragraph-id
        pattern=ParagraphIdPattern,
        min_length=32 + 1 + 1 + 1 + 1 + 0 + 0 + 1 + 3,
        # max field id of 250 and 10 digit paragraphs. More than enough
        max_length=32 + 1 + 1 + 1 + 250 + 1 + 128 + 1 + 21,
    ),
]


# Request


class ResourceProp(str, Enum):
    """Superset of former `show` and `extracted` serializations options."""

    # `show` props
    BASIC = "basic"
    ORIGIN = "origin"
    EXTRA = "extra"
    RELATIONS = "relations"
    VALUES = "values"
    ERRORS = "errors"
    SECURITY = "security"
    # `extracted` props
    EXTRACTED_TEXT = "extracted_text"
    EXTRACTED_METADATA = "extracted_metadata"
    EXTRACTED_SHORTENED_METADATA = "extracted_shortened_metadata"
    EXTRACTED_LARGE_METADATA = "extracted_large_metadata"
    EXTRACTED_VECTOR = "extracted_vectors"
    EXTRACTED_LINK = "extracted_link"
    EXTRACTED_FILE = "extracted_file"
    EXTRACTED_QA = "extracted_question_answers"
    # new granular props
    TITLE = "title"
    SUMMARY = "summary"
    CLASSIFICATION_LABELS = "classification_labels"

    @classmethod
    def from_show_and_extracted(
        cls, show: list[ResourceProperties], extracted: list[ExtractedDataTypeName]
    ) -> list["ResourceProp"]:
        _show_to_prop = {
            ResourceProperties.BASIC: cls.BASIC,
            ResourceProperties.ORIGIN: cls.ORIGIN,
            ResourceProperties.EXTRA: cls.EXTRA,
            ResourceProperties.RELATIONS: cls.RELATIONS,
            ResourceProperties.VALUES: cls.VALUES,
            ResourceProperties.ERRORS: cls.ERRORS,
            ResourceProperties.SECURITY: cls.SECURITY,
        }
        _extracted_to_prop = {
            ExtractedDataTypeName.TEXT: cls.EXTRACTED_TEXT,
            ExtractedDataTypeName.METADATA: cls.EXTRACTED_METADATA,
            ExtractedDataTypeName.SHORTENED_METADATA: cls.EXTRACTED_SHORTENED_METADATA,
            ExtractedDataTypeName.LARGE_METADATA: cls.EXTRACTED_LARGE_METADATA,
            ExtractedDataTypeName.VECTOR: cls.EXTRACTED_VECTOR,
            ExtractedDataTypeName.LINK: cls.EXTRACTED_LINK,
            ExtractedDataTypeName.FILE: cls.EXTRACTED_FILE,
            ExtractedDataTypeName.QA: cls.EXTRACTED_QA,
        }

        props = []
        for s in show:
            show_prop = _show_to_prop.get(s)
            # show=extracted is not in the dict
            if show_prop is None:
                continue
            props.append(show_prop)

        if ResourceProperties.EXTRACTED in show:
            for e in extracted:
                extracted_prop = _extracted_to_prop[e]
                props.append(extracted_prop)

        return props


class AugmentResourceFields(BaseModel):
    text: bool = False
    classification_labels: bool = False

    filters: list[filters.FieldId | filters.Generated]

    # TODO: review if this is equivalent

    # field_ids: list[str]
    # data_augmentation_field_prefixes: list[str]
    # field_types: list[str]


class AugmentResources(BaseModel):
    given: list[ResourceId]

    select: list[ResourceProp] = Field(default_factory=list)

    field_type_filter: list[FieldTypeName] | None = Field(
        default=None,
        deprecated="Only use this for legacy resource serialization",
        title="Field type filter",
        description=(
            "Define which field types are serialized on resources of search results. "
            "If omitted and legacy serialization is used, all field types will be serialized"
        ),
    )

    fields: AugmentResourceFields | None = None

    @model_validator(mode="after")
    def bwc_resource_serialization(self) -> Self:
        if self.field_type_filter is not None and self.fields is not None:
            raise ValueError("`field_type_filter` and `fields` are incompatible together")

        return self


class AugmentFields(BaseModel):
    given: list[FieldId]

    text: bool = False
    classification_labels: bool = False
    entities: bool = False  # also known as ners


class ParagraphMetadata(BaseModel):
    field_labels: list[str]
    paragraph_labels: list[str]

    is_an_image: bool
    is_a_table: bool

    # for extracted from visual content (ocr, inception, tables)
    source_file: str | None

    # for documents (pdf, docx...) only
    page: int | None
    in_page_with_visual: bool | None


class AugmentParagraph(BaseModel):
    id: ParagraphId
    metadata: ParagraphMetadata | None = None


class AugmentParagraphs(BaseModel):
    given: list[AugmentParagraph]

    text: bool = True

    neighbours_before: int = 0
    neighbours_after: int = 0

    # paragraph extracted from an image, return an image
    source_image: bool = False

    # paragraph extracted from a table, return table image
    table_image: bool = False

    # return page_preview instead of table image if table image enabled
    table_prefers_page_preview: bool = False

    # paragraph from a page, return page preview image
    page_preview_image: bool = False


class AugmentRequest(BaseModel):
    resources: AugmentResources | None = None
    fields: AugmentFields | None = None
    paragraphs: AugmentParagraphs | None = None


# Response


class AugmentedParagraph(BaseModel):
    text: str | None = None

    neighbours_before: dict[ParagraphId, str] | None = None
    neighbours_after: dict[ParagraphId, str] | None = None

    image: Image | None = None


class AugmentedField(BaseModel):
    text: str | None = None

    classification_labels: dict[str, list[str]] | None = None

    # former ners
    entities: dict[str, list[str]] | None = None

    page_preview_image: Image | None = None


class AugmentedResource(Resource):
    classification_labels: dict[str, list[str]] | None = None

    def updated_from(self, origin: Resource):
        for key in origin.model_fields.keys():
            self.__setattr__(key, getattr(origin, key))


class AugmentResponse(BaseModel):
    resources: dict[ResourceId, AugmentedResource]
    fields: dict[FieldId, AugmentedField]
    paragraphs: dict[ParagraphId, AugmentedParagraph]
