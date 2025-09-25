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
from typing import Annotated, Optional, Union

from pydantic import BaseModel, Field, StringConstraints

from nucliadb_models.common import FieldTypeName
from nucliadb_models.metadata import Origin
from nucliadb_models.resource import FieldConversation, FieldFile, FieldLink, FieldText
from nucliadb_models.search import Image
from nucliadb_models.security import ResourceSecurity


class ResourceHydration(BaseModel, extra="forbid"):
    title: bool = Field(
        default=True,
        description="Hydrate resource titles",
    )
    summary: bool = Field(
        default=False,
        description="Hydrate resource summaries",
    )

    origin: bool = Field(
        default=False,
        description="Hydrate resource origin",
    )

    security: bool = Field(
        default=False,
        description="Hydrate resource security metadata",
    )


class TextFieldHydration(BaseModel, extra="forbid"):
    value: bool = Field(
        default=False,
        description="Hydrate text field values. Field values are similar payloads to the ones used to create them",
    )
    extracted_text: bool = Field(
        default=False,
        description="Hydrate extracted text for text fields",
    )
    # TODO: what else should be interesting to add?


class FileFieldHydration(BaseModel, extra="forbid"):
    value: bool = Field(
        default=False,
        description="Hydrate file field values. Field values are similar payloads to the ones used to create them",
    )
    extracted_text: bool = Field(
        default=False,
        description="Hydrate extracted text for file fields",
    )
    # TODO: what else should be interesting to add?


class LinkFieldHydration(BaseModel, extra="forbid"):
    value: bool = Field(
        default=False,
        description="Hydrate link field values. Field values are similar payloads to the ones used to create them",
    )
    extracted_text: bool = Field(
        default=False,
        description="Hydrate extracted text for link fields",
    )
    # TODO: what else should be interesting to add?


class ConversationFieldHydration(BaseModel, extra="forbid"):
    value: bool = Field(
        default=False,
        description="Hydrate conversation field values. Field values are similar payloads to the ones used to create them",
    )

    # TODO: add fields to hydrate conversation fields. Think about how to handle
    # splits and fulfill the conversational RAG strategies

    # TODO: what else should be interesting to add?


class GenericFieldHydration(BaseModel, extra="forbid"):
    value: bool = Field(
        default=False,
        description="Hydrate generic field values. Field values are similar payloads to the ones used to create them",
    )
    extracted_text: bool = Field(
        default=False,
        description="Hydrate extracted text for generic fields",
    )
    # TODO: what else should be interesting to add?


class FieldHydration(BaseModel, extra="forbid"):
    text: Optional[TextFieldHydration] = Field(
        default_factory=TextFieldHydration,
        description="Text fields hydration options",
    )
    file: Optional[FileFieldHydration] = Field(
        default_factory=FileFieldHydration,
        description="File fields hydration options",
    )
    link: Optional[LinkFieldHydration] = Field(
        default_factory=LinkFieldHydration,
        description="Link fields hydration options",
    )
    conversation: Optional[ConversationFieldHydration] = Field(
        default_factory=ConversationFieldHydration,
        description="Conversation fields hydration options",
    )
    generic: Optional[GenericFieldHydration] = Field(
        default_factory=GenericFieldHydration,
        description="Generic fields hydration options",
    )


class NeighbourParagraphHydration(BaseModel, extra="forbid"):
    before: int = Field(
        default=2,
        ge=0,
        description="Number of previous paragraphs to hydrate",
    )
    after: int = Field(
        default=2,
        ge=0,
        description="Number of following paragraphs to hydrate",
    )


class RelatedParagraphHydration(BaseModel, extra="forbid"):
    neighbours: Optional[NeighbourParagraphHydration] = Field(
        default=None,
        description="Hydrate extra paragraphs that surround the original one",
    )

    # TODO: FEATURE: implement related paragraphs by page
    # page: bool = Field(
    #     default=False,
    #     description="Hydrate all paragraphs in the same page. This only applies to fields with pages",
    # )

    # TODO: description
    # XXX: should we let users control the amount of elements?
    parents: bool = False
    # TODO: description
    # XXX: should we let users control the amount of elements?
    siblings: bool = False
    # TODO: description
    # XXX: should we let users control the amount of elements?
    replacements: bool = False


class ImageParagraphHydration(BaseModel, extra="forbid"):
    # The source image is also known as reference or reference_file in the
    # paragraph context. The reference/reference_file is the filename of the
    # source image from which the paragraph has been extracted
    source_image: bool = Field(
        default=False,
        description=(
            "When a paragraph has been extracted from an image (using OCR, inception...), "
            "hydrate the image that represents it"
        ),
    )


class TableParagraphHydration(BaseModel, extra="forbid"):
    # TODO: implement. ARAG uses the label "/k/table" to check whether a
    # paragraph is or a table or not. We can also use info on maindb
    table_page_preview: bool = Field(
        default=False,
        description="Hydrate the page preview for the table. This will only hydrate fields with pages",
    )


class ParagraphPageHydration(BaseModel, extra="forbid"):
    # For some field types (file and link) learning generates previews. A
    # preview is a PDF file representing the content. For a docx for example, is
    # the PDF equivalent. Depending on the field type, the preview can
    # represent, for example, a page in a document or a portion of a webpage.
    page_with_visual: bool = Field(
        default=False,
        description=(
            "When a paragraph has been extracted from a page containing visual "
            "content (images, tables...), hydrate the preview of the paragraph's "
            "page as an image. Not all field types have previews nor visual content"
        ),
    )


class ParagraphHydration(BaseModel, extra="forbid"):
    text: bool = Field(
        default=True,
        description="Hydrate paragraph text",
    )
    image: Optional[ImageParagraphHydration] = Field(
        default=None,
        description="Hydrate options for paragraphs extracted from images (using OCR, inception...)",
    )
    table: Optional[TableParagraphHydration] = Field(
        default=None,
        description="Hydrate options for paragraphs extracted from tables",
    )

    # TODO: at some point, we should add hydration options for paragraphs from
    # audio and video

    page: Optional[ParagraphPageHydration] = Field(
        default=None,
        description="Hydrte options for paragraphs within a page. This applies to paragraphs in fields with pages",
    )

    related: Optional[RelatedParagraphHydration] = Field(
        default=None,
        description="Hydration options for related paragraphs. For example, neighbours or sibling paragraphs",
    )


class Hydration(BaseModel, extra="forbid"):
    resource: Optional[ResourceHydration] = Field(
        default_factory=ResourceHydration,
        description="Resource hydration options",
    )
    field: FieldHydration = Field(
        default_factory=FieldHydration,
        description="Field hydration options",
    )
    paragraph: ParagraphHydration = Field(
        default_factory=ParagraphHydration,
        description="Paragraph hydration options",
    )


ParagraphId = Annotated[
    str,
    StringConstraints(
        pattern=r"^[0-9a-f]{32}/[acftu]/[a-zA-Z0-9:_-]+/[0-9]+-[0-9]+$",
        min_length=32 + 1 + 1 + 1 + 1 + 1 + 3,
        # max field id of 250 and 10 digit paragraphs. More than enough
        max_length=32 + 1 + 1 + 1 + 250 + 1 + 21,
    ),
]


class HydrateRequest(BaseModel, extra="forbid"):
    data: list[ParagraphId] = Field(
        description="List of paragraph ids we want to hydrate",
        max_length=50,
    )
    hydration: Hydration = Field(description="Description of how hydration must be performed")


### Response models


class HydratedResource(BaseModel, extra="forbid"):
    id: str = Field(description="Unique resource id")
    slug: str = Field(description="Resource slug")

    title: Optional[str] = None
    summary: Optional[str] = None

    origin: Optional[Origin] = None

    security: Optional[ResourceSecurity] = None

    # TODO: add resource labels to hydrated resources


class FieldExtractedData(BaseModel, extra="forbid"):
    text: Optional[str] = None


class SplitFieldExtractedData(BaseModel, extra="forbid"):
    texts: Optional[dict[str, str]] = None


class HydratedTextField(BaseModel, extra="forbid"):
    id: str = Field("Unique field id")
    resource: str = Field("Field resource id")
    field_type: FieldTypeName = FieldTypeName.TEXT

    value: Optional[FieldText] = None
    extracted: Optional[FieldExtractedData] = None


class HydratedFileField(BaseModel, extra="forbid"):
    id: str = Field("Unique field id")
    resource: str = Field("Field resource id")
    field_type: FieldTypeName = FieldTypeName.FILE

    value: Optional[FieldFile] = None
    extracted: Optional[FieldExtractedData] = None

    previews: Optional[dict[str, Image]] = Field(
        default=None,
        title="Previews of specific parts of the field",
        description=(
            "Previews for specific pages of this field. Previews are differents"
            "depending on the file type. For example, for a PDF file, a preview"
            "will be an image of a single page."
            "In this field, previews will be populated according to the hydration"
            "options requested."
        ),
    )


class HydratedLinkField(BaseModel, extra="forbid"):
    id: str = Field("Unique field id")
    resource: str = Field("Field resource id")
    field_type: FieldTypeName = FieldTypeName.LINK

    value: Optional[FieldLink] = None
    extracted: Optional[FieldExtractedData] = None


class HydratedConversationField(BaseModel, extra="forbid"):
    id: str = Field("Unique field id")
    resource: str = Field("Field resource id")
    field_type: FieldTypeName = FieldTypeName.CONVERSATION

    value: Optional[FieldConversation] = None
    extracted: Optional[FieldExtractedData] = None


class HydratedGenericField(BaseModel, extra="forbid"):
    id: str = Field("Unique field id")
    resource: str = Field("Field resource id")
    field_type: FieldTypeName = FieldTypeName.TEXT

    value: Optional[str] = None
    extracted: Optional[FieldExtractedData] = None


class RelatedNeighbourParagraphRefs(BaseModel, extra="forbid"):
    before: Optional[list[str]] = None
    after: Optional[list[str]] = None


class RelatedParagraphRefs(BaseModel, extra="forbid"):
    neighbours: Optional[RelatedNeighbourParagraphRefs] = None
    parents: Optional[list[str]] = None
    siblings: Optional[list[str]] = None
    replacements: Optional[list[str]] = None


class HydratedParagraphImage(BaseModel, extra="forbid"):
    source_image: Optional[Image] = Field(
        default=None,
        description=(
            "Source image for this paragraph. This only applies to paragraphs "
            "extracted from an image using OCR or inception, and if this "
            "hydration option has been enabled in the request"
        ),
    )


class HydratedParagraphTable(BaseModel, extra="forbid"):
    page_preview_ref: Optional[str] = Field(
        default=None,
        description=(
            "Referento to the page preview for this paragraph. The actual "
            "preview will be found in the previews of its field. This only "
            "applies to paragraphs generated from a table and if the "
            "corresponding hydration option has been enabled in the request"
        ),
    )


class HydratedParagraphPage(BaseModel, extra="forbid"):
    page_preview_ref: Optional[str] = Field(
        default=None,
        description=(
            "Reference to the page preview for this paragraph. The actual "
            "preview will be found in the previews of its field. This only "
            "applies to paragraphs extracted from a page containing visual "
            "content and if the corresponding hydration option has been enabled "
            "in the request"
        ),
    )


class HydratedParagraph(BaseModel, extra="forbid"):
    id: str = Field(description="Unique paragraph id")
    field: str = Field(description="Paragraph field id")
    resource: str = Field(description="Paragraph resource id")

    text: Optional[str] = None

    # TODO: add labels to hydrated paragraphs
    # labels: Optional[list[str]] = None

    related: Optional[RelatedParagraphRefs] = None

    image: Optional[HydratedParagraphImage] = None
    table: Optional[HydratedParagraphTable] = None
    page: Optional[HydratedParagraphPage] = None


class Hydrated(BaseModel, extra="forbid"):
    resources: dict[str, HydratedResource]
    fields: dict[
        str,
        Union[
            HydratedTextField,
            HydratedFileField,
            HydratedLinkField,
            HydratedConversationField,
            HydratedGenericField,
        ],
    ]
    paragraphs: dict[str, HydratedParagraph]
