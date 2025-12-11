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
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Discriminator, Field, Tag, model_validator
from typing_extensions import Self

import nucliadb_models
from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb_models import filters, hydration
from nucliadb_models.augment import ResourceId
from nucliadb_models.common import FieldTypeName
from nucliadb_models.conversation import FieldConversation
from nucliadb_models.file import FieldFile
from nucliadb_models.link import FieldLink
from nucliadb_models.metadata import Extra, Origin
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import ResourceProperties, SearchParamDefaults
from nucliadb_protos import resources_pb2


class SelectProp(BaseModel):
    prop: Any

    @model_validator(mode="after")
    def set_discriminator(self) -> Self:
        # Ensure discriminator is explicitly set so it's always serialized
        self.prop = self.prop
        return self


def prop_discriminator(v: Any) -> str | None:
    if isinstance(v, dict):
        return v.get("prop", None)
    else:
        return getattr(v, "prop", None)


def from_discriminator(v: Any) -> str | None:
    if isinstance(v, dict):
        return v.get("from", None)
    else:
        return getattr(v, "from", None)


# Complex ids


class Metadata(BaseModel):
    is_an_image: bool
    is_a_table: bool

    # for extracted from visual content (ocr, inception, tables)
    source_file: str | None

    # for documents (pdf, docx...) only
    page: int | None
    in_page_with_visual: bool | None

    @classmethod
    def unknown(cls) -> Self:
        return cls(
            is_an_image=False,
            is_a_table=False,
            source_file=None,
            page=None,
            in_page_with_visual=None,
        )

    @classmethod
    def from_text_block_match(cls, text_block: TextBlockMatch) -> Self:
        return cls(
            is_an_image=text_block.is_an_image,
            is_a_table=text_block.is_a_table,
            source_file=text_block.representation_file,
            page=text_block.position.page_number,
            in_page_with_visual=text_block.page_with_visual,
        )

    @classmethod
    def from_db_paragraph(cls, paragraph: resources_pb2.Paragraph) -> Self:
        is_an_image = paragraph.kind not in (
            resources_pb2.Paragraph.TypeParagraph.OCR,
            resources_pb2.Paragraph.TypeParagraph.INCEPTION,
        )
        # REVIEW: can a paragraph be of a different type and still be a table?
        is_a_table = (
            paragraph.kind == resources_pb2.Paragraph.TypeParagraph.TABLE
            or paragraph.representation.is_a_table
        )

        if paragraph.representation.reference_file:
            source_file = paragraph.representation.reference_file
        else:
            source_file = None

        if paragraph.HasField("page"):
            page = paragraph.page.page
            in_page_with_visual = paragraph.page.page_with_visual
        else:
            page = None
            in_page_with_visual = None

        return cls(
            is_an_image=is_an_image,
            is_a_table=is_a_table,
            source_file=source_file,
            page=page,
            in_page_with_visual=in_page_with_visual,
        )


class Paragraph(BaseModel):
    id: ParagraphId
    metadata: Metadata | None = None

    @classmethod
    def from_text_block_match(cls, text_block: TextBlockMatch) -> Self:
        return cls(
            id=text_block.paragraph_id,
            metadata=Metadata.from_text_block_match(text_block),
        )

    @classmethod
    def from_db_paragraph(cls, id: ParagraphId, paragraph: resources_pb2.Paragraph) -> Self:
        return cls(
            id=id,
            metadata=Metadata.from_db_paragraph(paragraph),
        )


# SELECT props


class ParagraphText(SelectProp):
    prop: Literal["text"] = "text"


class ParagraphImage(SelectProp):
    prop: Literal["image"] = "image"


class ParagraphTable(SelectProp):
    prop: Literal["table"] = "table"

    # sometimes, due to a not perfect extraction, is better to use the page
    # preview instead of the table image for context. This options let users
    # choose
    prefer_page_preview: bool = False


class ParagraphPage(SelectProp):
    prop: Literal["page"] = "page"
    preview: bool = True


class RelatedParagraphs(SelectProp):
    prop: Literal["related"] = "related"
    neighbours: hydration.NeighbourParagraphHydration


ParagraphProp = Annotated[
    (
        Annotated[ParagraphText, Tag("text")]
        | Annotated[ParagraphImage, Tag("image")]
        | Annotated[ParagraphTable, Tag("table")]
        | Annotated[RelatedParagraphs, Tag("related")]
    ),
    Discriminator(prop_discriminator),
]


class FieldText(SelectProp):
    prop: Literal["text"] = "text"


class FieldValue(SelectProp):
    prop: Literal["value"] = "value"


class FieldClassificationLabels(SelectProp):
    prop: Literal["classification_labels"] = "classification_labels"


class FieldEntities(SelectProp):
    """Same as MetadataExtensionStrategy asking for ners"""

    prop: Literal["entities"] = "entities"


FieldProp = Annotated[
    (
        Annotated[FieldText, Tag("text")]
        | Annotated[FieldValue, Tag("value")]
        | Annotated[FieldClassificationLabels, Tag("classification_labels")]
        | Annotated[FieldEntities, Tag("entities")]
    ),
    Discriminator(prop_discriminator),
]


class ConversationTextStrategy(str, Enum):
    MESSAGE = "message"
    PAGE = "page"
    FULL = "full"


class ConversationText(FieldText):
    prop: Literal["text"] = "text"
    strategy: ConversationTextStrategy = ConversationTextStrategy.MESSAGE


class ConversationAttachments(SelectProp):
    prop: Literal["attachments"] = "attachments"
    text: bool
    image: bool


class ConversationAnswer(SelectProp):
    prop: Literal["answer"] = "answer"


ConversationProp = Annotated[
    (
        Annotated[ConversationText, Tag("text")]
        | Annotated[FieldValue, Tag("value")]
        | Annotated[FieldClassificationLabels, Tag("classification_labels")]
        | Annotated[FieldEntities, Tag("entities")]
        | Annotated[ConversationAttachments, Tag("attachments")]
        | Annotated[ConversationAnswer, Tag("answer")]
    ),
    Discriminator(prop_discriminator),
]


class ResourceTitle(SelectProp):
    prop: Literal["title"] = "title"


class ResourceSummary(SelectProp):
    prop: Literal["summary"] = "summary"


class ResourceBasic(SelectProp):
    """Same as show=["basic"] using GET resource or search endpoints"""

    prop: Literal["basic"] = "basic"


class ResourceOrigin(SelectProp):
    """Same as show=["origin"] using GET resource or search endpoints"""

    prop: Literal["origin"] = "origin"


class ResourceExtra(SelectProp):
    """Same as show=["extra"] and MetadataExtensionStrategy asking for
    extra_metadata

    """

    prop: Literal["extra"] = "extra"


class ResourceSecurity(SelectProp):
    """Same as show=["security"] using GET resource or search endpoints"""

    prop: Literal["security"] = "security"


class ResourceClassificationLabels(SelectProp):
    """Same as MetadataExtensionStrategy asking for classification_labels"""

    prop: Literal["classification_labels"] = "classification_labels"


class ResourceFieldsFilter(BaseModel):
    ids: list[str]


ResourceProp = Annotated[
    (
        Annotated[ResourceTitle, Tag("title")]
        | Annotated[ResourceSummary, Tag("summary")]
        | Annotated[ResourceBasic, Tag("basic")]
        | Annotated[ResourceOrigin, Tag("origin")]
        | Annotated[ResourceExtra, Tag("extra")]
        | Annotated[ResourceSecurity, Tag("security")]
        | Annotated[ResourceClassificationLabels, Tag("classification_labels")]
    ),
    Discriminator(prop_discriminator),
]


# Augmentations


class ResourceAugment(BaseModel, extra="forbid"):
    given: list[ResourceId | FieldId | ParagraphId]
    select: list[ResourceProp]
    from_: Literal["resources"] = Field(default="resources", alias="from")


class DeepResourceAugment(BaseModel, extra="forbid"):
    given: list[ResourceId]

    # old style serialization parameters
    show: list[ResourceProperties] = SearchParamDefaults.show.to_pydantic_field()
    extracted: list[ExtractedDataTypeName] = SearchParamDefaults.extracted.to_pydantic_field()
    field_type_filter: list[FieldTypeName] = SearchParamDefaults.field_type_filter.to_pydantic_field()

    from_: Literal["resources.deep"] = Field(default="resources.deep", alias="from")


class ConversationAugmentLimits(BaseModel):
    max_messages: int | None = Field(default=15, ge=0)


class ConversationAugment(BaseModel, extra="forbid"):
    given: list[FieldId | ParagraphId]
    select: list[ConversationProp]
    from_: Literal["conversations"] = Field(default="conversations", alias="from")
    limits: ConversationAugmentLimits | None = Field(default_factory=ConversationAugmentLimits)


class FieldAugment(BaseModel, extra="forbid"):
    given: list[ResourceId] | list[FieldId] | list[ParagraphId]
    select: list[FieldProp]
    from_: Literal["fields"] = Field(default="fields", alias="from")
    filter: list[filters.Field | filters.Generated] | None = None


class ParagraphAugment(BaseModel, extra="forbid"):
    given: list[Paragraph]
    select: list[ParagraphProp]
    from_: Literal["paragraphs"] = Field(default="paragraphs", alias="from")


class AugmentationLimits(BaseModel, extra="forbid"):
    # TODO: global augmentation limits (max chars, images, image size...)
    ...


Augment = Annotated[
    (
        Annotated[ResourceAugment, Tag("resources")]
        | Annotated[DeepResourceAugment, Tag("resources.deep")]
        | Annotated[FieldAugment, Tag("fields")]
        | Annotated[ConversationAugment, Tag("conversations")]
        | Annotated[ParagraphAugment, Tag("paragraphs")]
    ),
    Discriminator(from_discriminator),
]


class AugmentRequest(BaseModel, extra="forbid"):
    augmentations: list[Augment] = Field(
        default_factory=list,
        description="List of augmentations to be performed",
    )

    limits: AugmentationLimits | None = Field(
        default=None,
        description="Global hydration limits applied to the whole request",
    )


# Augmented data models


@dataclass
class AugmentedRelatedParagraphs:
    neighbours_before: list[ParagraphId]
    neighbours_after: list[ParagraphId]


@dataclass
class AugmentedParagraph:
    id: ParagraphId

    # textual representation of the paragraph
    text: str | None

    # original image for the paragraph when it has been extracted from an image
    # or a table. This value is the path to be used in the download endpoint
    source_image_path: str | None

    # if the paragraph comes from a page, this is the path for the download
    # endpoint to get the page preview image
    page_preview_path: str | None

    related: AugmentedRelatedParagraphs | None


@dataclass
class BaseAugmentedField:
    id: FieldId

    text: str | None = None

    classification_labels: dict[str, set[str]] | None = None
    entities: dict[str, set[str]] | None = None


@dataclass
class AugmentedTextField(BaseAugmentedField):
    value: nucliadb_models.text.FieldText | None = None


@dataclass
class AugmentedFileField(BaseAugmentedField):
    value: FieldFile | None = None


@dataclass
class AugmentedLinkField(BaseAugmentedField):
    value: FieldLink | None = None


@dataclass
class AugmentedConversationField(BaseAugmentedField):
    value: FieldConversation | None = None


@dataclass
class AugmentedGenericField(BaseAugmentedField):
    value: str | None = None


AugmentedField = (
    BaseAugmentedField
    | AugmentedTextField
    | AugmentedFileField
    | AugmentedLinkField
    | AugmentedConversationField
    | AugmentedGenericField
)


@dataclass
class AugmentedResource:
    id: str

    title: str | None
    summary: str | None

    origin: Origin | None
    extra: Extra | None
    security: nucliadb_models.security.ResourceSecurity | None

    classification_labels: dict[str, set[str]] | None


@dataclass
class Augmented:
    resources: dict[str, AugmentedResource]
    resources_deep: dict[str, Resource]
    fields: dict[FieldId, AugmentedField]
    paragraphs: dict[ParagraphId, AugmentedParagraph]
