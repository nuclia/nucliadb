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
from nucliadb_models.search import ResourceProperties

ResourceIdPattern = r"^([0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$"
ResourceId = Annotated[
    str,
    StringConstraints(pattern=ResourceIdPattern, min_length=32, max_length=36),
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

    filters: list[filters.Field | filters.Generated]


class AugmentResources(BaseModel):
    given: list[ResourceId]

    # TODO(decoupled-ask): replace this select for bool fields
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

    # For file fields, augment the path to the thumbnail image
    file_thumbnail: bool = False

    # When enabled, augment all the messages from the conversation. This is
    # incompatible with max_conversation_messages defined
    full_conversation: bool = False

    # When `full` disbled, this option controls the max amount of messages to be
    # augmented. This number will be a best-effort window centered around the
    # selected message. In addition, the 1st message of the conversation will
    # always be included.
    #
    # This option is combinable with attachments.
    max_conversation_messages: int | None = None

    # Given a message, if it's a question, try to find an answer. Otherwise,
    # return a window of messages following the requested one.
    #
    # This was previously done without explicit user consent, now it's an option.
    conversation_answer_or_messages_after: bool = False

    # Both attachment options will only add attachments for the full or the 1st
    # + window, not answer nor messages after

    # include conversation text attachments
    conversation_text_attachments: bool = False
    # include conversation image attachments
    conversation_image_attachments: bool = False

    @model_validator(mode="after")
    def validate_cross_options(self):
        if self.full_conversation and self.max_conversation_messages is not None:
            raise ValueError(
                "`full_conversation` and `max_conversation_messages` are not compatible together"
            )
        if (
            (self.conversation_text_attachments or self.conversation_image_attachments)
            and self.full_conversation is False
            and self.max_conversation_messages is None
        ):
            raise ValueError(
                "Attachments are only compatible with `full_conversation` and `max_conversation_messages`"
            )
        return self


# TODO(decoupled-ask): remove unused metadata
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

    @model_validator(mode="after")
    def table_options_work_together(self) -> Self:
        if not self.table_image and self.table_prefers_page_preview:
            raise ValueError("`table_prefers_page_preview` can only be enabled with `table_image`")
        return self


class AugmentRequest(BaseModel):
    resources: AugmentResources | None = None
    fields: AugmentFields | None = None
    paragraphs: AugmentParagraphs | None = None


# Response


class AugmentedParagraph(BaseModel):
    text: str | None = None

    neighbours_before: list[ParagraphId] | None = None
    neighbours_after: list[ParagraphId] | None = None

    source_image: str | None = None
    table_image: str | None = None
    page_preview_image: str | None = None


class AugmentedField(BaseModel):
    text: str | None = None

    classification_labels: dict[str, list[str]] | None = None

    # former ners
    entities: dict[str, list[str]] | None = None


class AugmentedFileField(BaseModel):
    text: str | None = None

    classification_labels: dict[str, list[str]] | None = None

    # former ners
    entities: dict[str, list[str]] | None = None

    # TODO(decoupled-ask): implement image strategy
    page_preview_image: str | None = None

    # Path for the download API to retrieve the file thumbnail image
    thumbnail_image: str | None = None


class AugmentedConversationMessage(BaseModel):
    ident: str
    text: str | None = None
    attachments: list[FieldId] | None = None


class AugmentedConversationField(BaseModel):
    classification_labels: dict[str, list[str]] | None = None
    # former ners
    entities: dict[str, list[str]] | None = None

    messages: list[AugmentedConversationMessage] | None = None

    @property
    def text(self) -> str | None:
        """Syntactic sugar to access aggregate text from all messages"""
        if self.messages is None:
            return None

        text = ""
        for message in self.messages:
            text += message.text or ""

        return text or None

    @property
    def attachments(self) -> list[FieldId] | None:
        """Syntactic sugar to access the aggregate of attachments from all messages."""
        if self.messages is None:
            return None

        has_attachments = False
        attachments = []
        for message in self.messages:
            if message.attachments is None:
                continue
            has_attachments = True
            attachments.extend(message.attachments)

        if has_attachments:
            return attachments
        else:
            return None


class AugmentedResource(Resource):
    classification_labels: dict[str, list[str]] | None = None

    def updated_from(self, origin: Resource):
        for key in origin.model_fields.keys():
            self.__setattr__(key, getattr(origin, key))


class AugmentResponse(BaseModel):
    resources: dict[ResourceId, AugmentedResource]
    fields: dict[FieldId, AugmentedField | AugmentedFileField | AugmentedConversationField]
    paragraphs: dict[ParagraphId, AugmentedParagraph]
