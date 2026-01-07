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

from typing import Annotated

from pydantic import BaseModel, Field, StringConstraints, model_validator
from typing_extensions import Self, assert_never

from nucliadb_models import filters
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import ResourceProperties, TextPosition

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


class AugmentResourceFields(BaseModel):
    text: bool = False
    classification_labels: bool = False

    filters: list[filters.Field | filters.Generated]


class AugmentResources(BaseModel):
    given: list[ResourceId]

    # `show` props
    basic: bool = False
    origin: bool = False
    extra: bool = False
    relations: bool = False
    values: bool = False
    errors: bool = False
    security: bool = False

    # `extracted` props
    extracted_text: bool = False
    extracted_metadata: bool = False
    extracted_shortened_metadata: bool = False
    extracted_large_metadata: bool = False
    extracted_vector: bool = False
    extracted_link: bool = False
    extracted_file: bool = False
    extracted_qa: bool = False

    # new granular props
    title: bool = False
    summary: bool = False
    classification_labels: bool = False

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

    def apply_show_and_extracted(
        self, show: list[ResourceProperties], extracted: list[ExtractedDataTypeName]
    ):
        show_extracted = False
        for s in show:
            if s == ResourceProperties.BASIC:
                self.basic = True
            elif s == ResourceProperties.ORIGIN:
                self.origin = True
            elif s == ResourceProperties.EXTRA:
                self.extra = True
            elif s == ResourceProperties.RELATIONS:
                self.relations = True
            elif s == ResourceProperties.VALUES:
                self.values = True
            elif s == ResourceProperties.ERRORS:
                self.errors = True
            elif s == ResourceProperties.SECURITY:
                self.security = True
            elif s == ResourceProperties.EXTRACTED:
                show_extracted = True
            else:  # pragma: no cover
                assert_never(s)

        if show_extracted:
            for e in extracted:
                if e == ExtractedDataTypeName.TEXT:
                    self.extracted_text = True
                elif e == ExtractedDataTypeName.METADATA:
                    self.extracted_metadata = True
                elif e == ExtractedDataTypeName.SHORTENED_METADATA:
                    self.extracted_shortened_metadata = True
                elif e == ExtractedDataTypeName.LARGE_METADATA:
                    self.extracted_large_metadata = True
                elif e == ExtractedDataTypeName.VECTOR:
                    self.extracted_vector = True
                elif e == ExtractedDataTypeName.LINK:
                    self.extracted_link = True
                elif e == ExtractedDataTypeName.FILE:
                    self.extracted_file = True
                elif e == ExtractedDataTypeName.QA:
                    self.extracted_qa = True
                else:  # pragma: no cover
                    assert_never(s)


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


class ParagraphMetadata(BaseModel):
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
    resources: list[AugmentResources] | None = Field(default=None, min_length=1)
    fields: list[AugmentFields] | None = Field(default=None, min_length=1)
    paragraphs: list[AugmentParagraphs] | None = Field(default=None, min_length=1)


# Response


class AugmentedParagraph(BaseModel):
    text: str | None = None
    position: TextPosition | None = None

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
