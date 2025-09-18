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
from typing import Literal, Optional, Union, overload

from fastapi import Request, Response
from fastapi_versioning import version

from nucliadb.common.ids import FIELD_TYPE_STR_TO_NAME, FIELD_TYPE_STR_TO_PB, FieldId, ParagraphId
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.resource import Resource
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search import cache, paragraphs
from nucliadb.search.search.cache import request_caches
from nucliadb.search.search.hydrator import hydrate_field_text
from nucliadb.search.search.hydrator.images import download_image, download_page_preview
from nucliadb.search.search.hydrator.paragraphs import ParagraphIndex, related_paragraphs_refs
from nucliadb_models import hydration as hydration_models
from nucliadb_models.common import FieldTypeName
from nucliadb_models.hydration import Hydrated, HydrateRequest, Hydration
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import Image
from nucliadb_models.security import ResourceSecurity
from nucliadb_protos import resources_pb2
from nucliadb_utils.authentication import requires


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/hydrate",
    status_code=200,
    summary="Hydrate a set of paragraphs",
    description="Internal API endpoint to hydrate a set of paragraphs",
    include_in_schema=False,
    response_model_exclude_unset=True,
    tags=["Hydration"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def hydrate_endpoint(
    request: Request,
    response: Response,
    kbid: str,
    item: HydrateRequest,
) -> Hydrated:
    with request_caches():
        return await Hydrator(kbid, item.hydration).hydrate(item.data)


class HydratedBuilder:
    """Builder class to construct an Hydrated payload."""

    def __init__(self) -> None:
        self._resources: dict[str, hydration_models.HydratedResource] = {}
        self._fields: dict[
            str,
            Union[
                hydration_models.HydratedTextField,
                hydration_models.HydratedFileField,
                hydration_models.HydratedLinkField,
                hydration_models.HydratedConversationField,
                hydration_models.HydratedGenericField,
            ],
        ] = {}
        self._paragraphs: dict[str, hydration_models.HydratedParagraph] = {}

    @property
    def resources(self) -> dict[str, hydration_models.HydratedResource]:
        return self._resources

    @property
    def fields(
        self,
    ) -> dict[
        str,
        Union[
            hydration_models.HydratedTextField,
            hydration_models.HydratedFileField,
            hydration_models.HydratedLinkField,
            hydration_models.HydratedConversationField,
            hydration_models.HydratedGenericField,
        ],
    ]:
        return self._fields

    @property
    def paragraphs(self) -> dict[str, hydration_models.HydratedParagraph]:
        return self._paragraphs

    def build(self) -> Hydrated:
        return Hydrated(
            resources=self._resources,
            fields=self._fields,
            paragraphs=self._paragraphs,
        )

    def add_resource(self, rid: str, slug: str) -> hydration_models.HydratedResource:
        hydrated = hydration_models.HydratedResource(id=rid, slug=slug)
        self._resources[rid] = hydrated
        return hydrated

    @overload
    def add_field(
        self, field_id: FieldId, field_type: Literal[FieldTypeName.TEXT]
    ) -> hydration_models.HydratedTextField: ...

    @overload
    def add_field(
        self, field_id: FieldId, field_type: Literal[FieldTypeName.FILE]
    ) -> hydration_models.HydratedFileField: ...

    @overload
    def add_field(
        self, field_id: FieldId, field_type: Literal[FieldTypeName.LINK]
    ) -> hydration_models.HydratedLinkField: ...

    @overload
    def add_field(
        self, field_id: FieldId, field_type: Literal[FieldTypeName.CONVERSATION]
    ) -> hydration_models.HydratedConversationField: ...

    @overload
    def add_field(
        self, field_id: FieldId, field_type: Literal[FieldTypeName.GENERIC]
    ) -> hydration_models.HydratedGenericField: ...

    def add_field(self, field_id: FieldId, field_type: FieldTypeName):
        hydrated: Union[
            hydration_models.HydratedTextField,
            hydration_models.HydratedFileField,
            hydration_models.HydratedLinkField,
            hydration_models.HydratedConversationField,
            hydration_models.HydratedGenericField,
        ]

        if field_type == FieldTypeName.TEXT:
            hydrated = hydration_models.HydratedTextField(
                id=field_id.full(),
                resource=field_id.rid,
                field_type=field_type,
            )

        elif field_type == FieldTypeName.FILE:
            hydrated = hydration_models.HydratedFileField(
                id=field_id.full(),
                resource=field_id.rid,
                field_type=field_type,
            )

        elif field_type == FieldTypeName.LINK:
            hydrated = hydration_models.HydratedLinkField(
                id=field_id.full(),
                resource=field_id.rid,
                field_type=field_type,
            )

        elif field_type == FieldTypeName.CONVERSATION:
            hydrated = hydration_models.HydratedConversationField(
                id=field_id.full(),
                resource=field_id.rid,
                field_type=field_type,
            )

        elif field_type == FieldTypeName.GENERIC:
            hydrated = hydration_models.HydratedGenericField(
                id=field_id.full(),
                resource=field_id.rid,
                field_type=field_type,
            )

        else:  # pragma: no cover
            # This is a trick so mypy generates an error if this branch can be reached,
            # that is, if we are missing some ifs
            _a: int = "a"

        self._fields[field_id.full()] = hydrated
        return hydrated

    def new_paragraph(self, paragraph_id: ParagraphId) -> hydration_models.HydratedParagraph:
        return hydration_models.HydratedParagraph(
            id=paragraph_id.full(),
            field=paragraph_id.field_id.full(),
            resource=paragraph_id.rid,
        )

    def add_paragraph(self, paragraph_id: ParagraphId, paragraph: hydration_models.HydratedParagraph):
        self._paragraphs[paragraph_id.full()] = paragraph

    def page_preview_id(self, page: int) -> str:
        return f"{page}"

    def add_page_preview(self, paragraph_id: ParagraphId, page: int, image: Image):
        field_id = paragraph_id.field_id
        field = self._fields[field_id.full()]

        if not isinstance(field, hydration_models.HydratedFileField):
            # Other field types have no page preview concept
            return

        if field.previews is None:
            field.previews = {}

        preview_id = self.page_preview_id(page)
        field.previews[preview_id] = image

        paragraph = self._paragraphs[paragraph_id.full()]
        assert paragraph.page is not None, "should already be set"
        paragraph.page.page_preview_ref = preview_id

    def add_table_page_preview(self, paragraph_id: ParagraphId, page: int, image: Image):
        field_id = paragraph_id.field_id
        field = self._fields[field_id.full()]

        if not isinstance(field, hydration_models.HydratedFileField):
            # Other field types have no page preview concept
            return

        if field.previews is None:
            field.previews = {}

        preview_id = self.page_preview_id(page)
        field.previews[preview_id] = image

        paragraph = self._paragraphs[paragraph_id.full()]
        assert paragraph.table is not None, "should already be set"
        paragraph.table.page_preview_ref = preview_id


@dataclass
class ExtraParagraphHydration:
    field_page: Optional[int]
    field_table_page: Optional[int]
    related_paragraph_ids: list[ParagraphId]


class Hydrator:
    def __init__(self, kbid: str, config: Hydration):
        self.kbid = kbid
        self.config = config
        self.hydrated = HydratedBuilder()

        # cached paragraphs per field
        self.field_paragraphs: dict[FieldId, ParagraphIndex] = {}

    async def hydrate(self, paragraph_ids: list[str]) -> Hydrated:
        unique_paragraph_ids = set(paragraph_ids)
        for user_paragraph_id in unique_paragraph_ids:
            try:
                paragraph_id = ParagraphId.from_string(user_paragraph_id)
            except ValueError:
                # skip paragraphs with invalid format
                continue

            field_id = paragraph_id.field_id
            rid = paragraph_id.rid

            resource = await cache.get_resource(self.kbid, rid)
            if resource is None:
                # skip resources that aren't in the DB
                continue

            field_type_pb = FIELD_TYPE_STR_TO_PB[field_id.type]
            if not (await resource.field_exists(field_type_pb, field_id.key)):
                # skip a fields that aren't in the DB
                continue
            field = await resource.get_field(field_id.key, field_id.pb_type)

            # hydrate paragraphs (requested and related)

            (hydrated_paragraph, extra) = await self._hydrate_paragraph(
                resource, field, paragraph_id, self.config.paragraph
            )
            self.hydrated.add_paragraph(paragraph_id, hydrated_paragraph)

            for related_paragraph_id in extra.related_paragraph_ids:
                (hydrated_paragraph, _) = await self._hydrate_paragraph(
                    resource,
                    field,
                    related_paragraph_id,
                    hydration_models.ParagraphHydration(
                        text=self.config.paragraph.text, image=None, table=None, page=None, related=None
                    ),
                )
                self.hydrated.add_paragraph(related_paragraph_id, hydrated_paragraph)

            # hydrate field

            if field_id.full() not in self.hydrated.fields:
                await self._hydrate_field(resource, field_id)

            # hydrate other pages requested for this field

            # TODO: skip if already hydrated
            if extra.field_page is not None:
                preview = await download_page_preview(field, extra.field_page)
                if preview is not None:
                    self.hydrated.add_page_preview(paragraph_id, extra.field_page, preview)

            # TODO: skip if already hydrated
            if extra.field_table_page is not None:
                preview = await download_page_preview(field, extra.field_table_page)
                if preview is not None:
                    self.hydrated.add_table_page_preview(paragraph_id, extra.field_table_page, preview)

            # hydrate resource

            if rid not in self.hydrated.resources and self.config.resource is not None:
                await self._hydrate_resource(resource, rid, self.config.resource)

        return self.hydrated.build()

    async def _hydrate_resource(
        self, resource: Resource, rid: str, config: hydration_models.ResourceHydration
    ):
        basic = await resource.get_basic()

        slug = basic.slug
        hydrated = self.hydrated.add_resource(rid, slug)

        if config.title:
            hydrated.title = basic.title
        if config.summary:
            hydrated.summary = basic.summary

        if config.security:
            security = await resource.get_security()
            hydrated.security = ResourceSecurity(access_groups=[])
            if security is not None:
                for group_id in security.access_groups:
                    hydrated.security.access_groups.append(group_id)

        if config.origin:
            origin = await resource.get_origin()
            if origin is not None:
                # TODO: we want a better hydration than proto to JSON
                hydrated.origin = from_proto.origin(origin)

        return hydrated

    async def _hydrate_field(self, resource: Resource, field_id: FieldId):
        field_type = FIELD_TYPE_STR_TO_NAME[field_id.type]

        if field_type == FieldTypeName.TEXT:
            if not self.config.field.text is not None:
                # REVIEW: we still need to add the field, as a paragraph can
                # trigger hydration of some part of it even if it has no
                # specific hydration
                self.hydrated.add_field(field_id, field_type)
                return
            await self._hydrate_text_field(resource, field_id, self.config.field.text)

        elif field_type == FieldTypeName.FILE is not None:
            if not self.config.field.file:
                # REVIEW: we still need to add the field, as a paragraph can
                # trigger hydration of some part of it even if it has no
                # specific hydration
                self.hydrated.add_field(field_id, field_type)
                return
            await self._hydrate_file_field(resource, field_id, self.config.field.file)

        elif field_type == FieldTypeName.LINK is not None:
            if not self.config.field.link:
                # REVIEW: we still need to add the field, as a paragraph can
                # trigger hydration of some part of it even if it has no
                # specific hydration
                self.hydrated.add_field(field_id, field_type)
                return
            await self._hydrate_link_field(resource, field_id, self.config.field.link)

        elif field_type == FieldTypeName.CONVERSATION is not None:
            if not self.config.field.conversation:
                # REVIEW: we still need to add the field, as a paragraph can
                # trigger hydration of some part of it even if it has no
                # specific hydration
                self.hydrated.add_field(field_id, field_type)
                return
            await self._hydrate_conversation_field(resource, field_id, self.config.field.conversation)

        elif field_type == FieldTypeName.GENERIC is not None:
            if not self.config.field.generic:
                # REVIEW: we still need to add the field, as a paragraph can
                # trigger hydration of some part of it even if it has no
                # specific hydration
                self.hydrated.add_field(field_id, field_type)
                return
            await self._hydrate_generic_field(resource, field_id, self.config.field.generic)

        else:  # pragma: no cover
            # This is a trick so mypy generates an error if this branch can be reached,
            # that is, if we are missing some ifs
            _a: int = "a"

    async def _hydrate_text_field(
        self,
        resource: Resource,
        field_id: FieldId,
        config: hydration_models.TextFieldHydration,
    ) -> hydration_models.HydratedTextField:
        hydrated = self.hydrated.add_field(field_id, FieldTypeName.TEXT)

        if config.extracted_text:
            field_text = await hydrate_field_text(self.kbid, field_id)
            if field_text is not None:
                (_, text) = field_text
                hydrated.extracted = hydration_models.FieldExtractedData(text=text)

        return hydrated

    async def _hydrate_file_field(
        self,
        resource: Resource,
        field_id: FieldId,
        config: hydration_models.FileFieldHydration,
    ) -> hydration_models.HydratedFileField:
        hydrated = self.hydrated.add_field(field_id, FieldTypeName.FILE)

        if config.value:
            field = await resource.get_field(field_id.key, field_id.pb_type)
            value = await field.get_value()
            hydrated.value = from_proto.field_file(value)

        if config.extracted_text:
            field_text = await hydrate_field_text(self.kbid, field_id)
            if field_text is not None:
                (_, text) = field_text
                hydrated.extracted = hydration_models.FieldExtractedData(text=text)

        return hydrated

    async def _hydrate_link_field(
        self,
        resource: Resource,
        field_id: FieldId,
        config: hydration_models.LinkFieldHydration,
    ) -> hydration_models.HydratedLinkField:
        hydrated = self.hydrated.add_field(field_id, FieldTypeName.LINK)

        if config.value:
            field = await resource.get_field(field_id.key, field_id.pb_type)
            value = await field.get_value()
            hydrated.value = from_proto.field_link(value)

        if config.extracted_text:
            field_text = await hydrate_field_text(self.kbid, field_id)
            if field_text is not None:
                (_, text) = field_text
                hydrated.extracted = hydration_models.FieldExtractedData(text=text)

        return hydrated

    async def _hydrate_conversation_field(
        self,
        resource: Resource,
        field_id: FieldId,
        config: hydration_models.ConversationFieldHydration,
    ) -> hydration_models.HydratedConversationField:
        hydrated = self.hydrated.add_field(field_id, FieldTypeName.CONVERSATION)
        # TODO: implement conversation fields
        return hydrated

    async def _hydrate_generic_field(
        self,
        resource: Resource,
        field_id: FieldId,
        config: hydration_models.GenericFieldHydration,
    ) -> hydration_models.HydratedGenericField:
        hydrated = self.hydrated.add_field(field_id, FieldTypeName.GENERIC)

        if config.value:
            field = await resource.get_field(field_id.key, field_id.pb_type)
            value = await field.get_value()
            hydrated.value = value

        if config.extracted_text:
            field_text = await hydrate_field_text(self.kbid, field_id)
            if field_text is not None:
                (_, text) = field_text
                hydrated.extracted = hydration_models.FieldExtractedData(text=text)

        return hydrated

    async def _hydrate_paragraph(
        self,
        resource: Resource,
        field: Field,
        paragraph_id: ParagraphId,
        config: hydration_models.ParagraphHydration,
    ) -> tuple[hydration_models.HydratedParagraph, ExtraParagraphHydration]:
        """This function assumes the paragraph field exists in the resource.
        However, the paragraph doesn't necessarily exist in the paragraph
        metadata, it can be made-up to include more or less text than the
        originally extracted.

        """

        # TODO: this should not add the paragraph to the hydrated payload, but maybe get a lock?
        hydrated = self.hydrated.new_paragraph(paragraph_id)
        extra_hydration = ExtraParagraphHydration(
            field_page=None, field_table_page=None, related_paragraph_ids=[]
        )

        if config.text:
            text = await paragraphs.get_paragraph_text(kbid=self.kbid, paragraph_id=paragraph_id)
            hydrated.text = text

        requires_field_metadata = config.image or config.table or config.page or config.related
        if requires_field_metadata:
            field_metadata = await field.get_field_metadata()
            if field_metadata is not None:
                # TODO: related paragraphs need an index to navigate relations, but
                # the rest of options are fine with a simple O(n) scan. We don't
                # need the index in those cases
                field_id = paragraph_id.field_id
                if field_id not in self.field_paragraphs:
                    index = ParagraphIndex()
                    await index.build(field_id, field_metadata)
                    self.field_paragraphs[field_id] = index
                field_paragraphs = self.field_paragraphs[field_id]

                paragraph = field_paragraphs.get(paragraph_id)
                if paragraph is not None:
                    # otherwise, this is a fake paragraph. We can't hydrate anything
                    # else here

                    if config.related:
                        hydrated.related, related_ids = await related_paragraphs_refs(
                            paragraph_id, field_paragraphs, config.related
                        )
                        extra_hydration.related_paragraph_ids = related_ids

                    if config.image:
                        hydrated.image = hydration_models.HydratedParagraphImage()

                        if config.image.source_image:
                            hydrated.image.source_image = await paragraph_source_image(
                                self.kbid, paragraph
                            )

                    if config.page:
                        if hydrated.page is None:
                            hydrated.page = hydration_models.HydratedParagraphPage()

                        if config.page.page_with_visual:
                            if paragraph.page.page_with_visual:
                                # Paragraphs can be found on pages with visual content. In this
                                # case, we want to return the preview of the paragraph page as
                                # an image
                                page_number = paragraph.page.page
                                # TODO: what should I do if I later find there's no page in the DB?
                                hydrated.page.page_preview_ref = self.hydrated.page_preview_id(
                                    page_number
                                )
                                extra_hydration.field_page = page_number

                    if config.table:
                        if hydrated.table is None:
                            hydrated.table = hydration_models.HydratedParagraphTable()

                        if config.table.table_page_preview:
                            if paragraph.representation.is_a_table:
                                # When a paragraph comes with a table and table hydration is
                                # enabled, we want to return the image representing that table.
                                # Ideally we should hydrate the paragraph reference_file, but
                                # table screenshots are not always perfect so we prefer to use
                                # the page preview. If at some point the table images are good
                                # enough, it'd be better to use those
                                page_number = paragraph.page.page
                                hydrated.table.page_preview_ref = self.hydrated.page_preview_id(
                                    page_number
                                )
                                extra_hydration.field_table_page = page_number

        return hydrated, extra_hydration


async def paragraph_source_image(kbid: str, paragraph: resources_pb2.Paragraph) -> Optional[Image]:
    """Certain paragraphs are extracted from images using techniques like OCR or
    inception. If that's the case, return the original image for this paragraph.

    """
    source_image = paragraph.representation.reference_file

    if paragraph.kind not in (
        resources_pb2.Paragraph.TypeParagraph.OCR,
        resources_pb2.Paragraph.TypeParagraph.INCEPTION,
    ):
        return None

    field_id = ParagraphId.from_string(paragraph.key).field_id

    # Paragraphs extracted from an image store its original image representation
    # in the reference file. The path is incomplete though, as it's stored in
    # the `generated` folder
    image = await download_image(
        kbid,
        field_id,
        f"generated/{source_image}",
        # XXX: we assume all reference files are PNG images, but this actually
        # depends on learning so it's a dangerous assumption. We should check it
        # by ourselves
        mime_type="image/png",
    )
    return image
