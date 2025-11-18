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

from typing_extensions import Self

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb_models.conversation import FieldConversation
from nucliadb_models.file import FieldFile
from nucliadb_models.link import FieldLink
from nucliadb_models.metadata import Extra, Origin
from nucliadb_models.security import ResourceSecurity
from nucliadb_models.text import FieldText
from nucliadb_protos import resources_pb2


@dataclass
class Metadata:
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


@dataclass
class Paragraph:
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


@dataclass
class RelatedParagraphs:
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

    related: RelatedParagraphs | None


@dataclass
class BaseAugmentedField:
    id: FieldId

    text: str | None = None

    # TODO: review tuples vs dict
    classification_labels: list[tuple[str, str]] | None = None
    # TODO: review tuples vs dict
    entities: dict[str, set[str]] | None = None


@dataclass
class AugmentedTextField(BaseAugmentedField):
    value: FieldText | None = None


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
    security: ResourceSecurity | None

    classification_labels: list[tuple[str, str]] | None


@dataclass
class Augmented:
    resources: dict[str, AugmentedResource]
    fields: dict[FieldId, AugmentedField]
    paragraphs: dict[ParagraphId, AugmentedParagraph]
