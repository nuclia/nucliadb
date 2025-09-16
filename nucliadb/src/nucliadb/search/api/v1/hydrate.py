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
import base64
from typing import Literal, Optional, Union, cast, overload

from fastapi import Request, Response
from fastapi_versioning import version

from nucliadb.common.ids import FIELD_TYPE_STR_TO_NAME, FieldId, ParagraphId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.serialize import get_orm_resource
from nucliadb.search import SERVICE_NAME
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search import paragraphs
from nucliadb.search.search.cache import request_caches
from nucliadb.search.search.hydrator import hydrate_field_text
from nucliadb_models import hydration as hydration_models
from nucliadb_models.common import FieldTypeName
from nucliadb_models.hydration import Hydrated, HydrateRequest, Hydration
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import Image
from nucliadb_models.security import ResourceSecurity
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import FieldComputedMetadata
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_storage


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

    def add_paragraph(self, paragraph_id: ParagraphId) -> hydration_models.HydratedParagraph:
        hydrated = hydration_models.HydratedParagraph(
            id=paragraph_id.full(),
            field=paragraph_id.field_id.full(),
            resource=paragraph_id.rid,
        )
        self._paragraphs[paragraph_id.full()] = hydrated
        return hydrated

    def add_page_preview(self, paragraph_id: ParagraphId, page: int, image: Image) -> str:
        field_id = paragraph_id.field_id
        field = self._fields[field_id.full()]
        # TODO: implement link previews
        assert isinstance(field, hydration_models.HydratedFileField), (
            f"field type '{field_id.type}' has no preview concept"
        )

        if field.previews is None:
            field.previews = {}

        preview_id = f"{page}"
        field.previews[preview_id] = image

        paragraph = self._paragraphs[paragraph_id.full()]
        assert paragraph.page is not None, "should already be set"
        paragraph.page.page_preview_ref = preview_id

        return preview_id

    def add_table_page_preview(self, paragraph_id: ParagraphId, page: int, image: Image) -> str:
        field_id = paragraph_id.field_id
        field = self._fields[field_id.full()]
        # TODO: implement link previews
        assert isinstance(field, hydration_models.HydratedFileField), (
            f"field type '{field_id.type}' has no preview concept"
        )

        if field.previews is None:
            field.previews = {}

        preview_id = f"{page}"
        field.previews[preview_id] = image

        paragraph = self._paragraphs[paragraph_id.full()]
        assert paragraph.table is not None, "should already be set"
        paragraph.table.page_preview_ref = preview_id

        return preview_id


class Hydrator:
    def __init__(self, kbid: str, config: Hydration):
        self.kbid = kbid
        self.config = config
        self.hydrated = HydratedBuilder()

        # cached paragraphs per field
        self.field_paragraphs: dict[FieldId, ParagraphIndex] = {}

    async def hydrate(self, paragraph_ids: list[str]) -> Hydrated:
        driver = get_driver()
        db_resources = {}
        async with driver.ro_transaction() as txn:
            for raw_paragraph_id in paragraph_ids:
                # TODO: remove this HACK skip
                if raw_paragraph_id.startswith(":"):
                    continue

                # TODO if we accept user ids, we must validate them
                paragraph_id = ParagraphId.from_string(raw_paragraph_id)
                field_id = paragraph_id.field_id
                rid = paragraph_id.rid

                if rid not in db_resources:
                    resource = await get_orm_resource(txn, self.kbid, rid, service_name=SERVICE_NAME)
                    assert resource is not None
                    db_resources[rid] = resource
                resource = db_resources[rid]

                if rid not in self.hydrated.resources:
                    await self._hydrate_resource(resource, rid)

                if field_id.full() not in self.hydrated.fields:
                    await self._hydrate_field(resource, field_id)

                # We can't skip paragraphs as we may have found a paragraph due
                # to a relation but, as a match, we'll hydrate more information
                await self._hydrate_paragraph(resource, paragraph_id)

        return self.hydrated.build()

    async def _hydrate_resource(self, resource: Resource, rid: str):
        basic = await resource.get_basic()

        slug = basic.slug
        hydrated = self.hydrated.add_resource(rid, slug)

        if self.config.resource.title:
            hydrated.title = basic.title
        if self.config.resource.summary:
            hydrated.summary = basic.summary

        if self.config.resource.security:
            security = await resource.get_security()
            hydrated.security = ResourceSecurity(access_groups=[])
            if security is not None:
                for group_id in security.access_groups:
                    hydrated.security.access_groups.append(group_id)

        if self.config.resource.origin:
            origin = await resource.get_origin()
            if origin is not None:
                # TODO: we want a better hydration than proto to JSON
                hydrated.origin = from_proto.origin(origin)

        return hydrated

    async def _hydrate_field(self, resource: Resource, field_id: FieldId):
        field_type = FIELD_TYPE_STR_TO_NAME[field_id.type]

        if field_type == FieldTypeName.TEXT:
            if not self.config.field.text:
                return
            await self._hydrate_text_field(resource, field_id, self.config.field.text)

        elif field_type == FieldTypeName.FILE:
            if not self.config.field.file:
                return
            await self._hydrate_file_field(resource, field_id, self.config.field.file)

        elif field_type == FieldTypeName.LINK:
            if not self.config.field.link:
                return
            await self._hydrate_link_field(resource, field_id, self.config.field.link)

        elif field_type == FieldTypeName.CONVERSATION:
            if not self.config.field.conversation:
                return
            await self._hydrate_conversation_field(resource, field_id, self.config.field.conversation)

        elif field_type == FieldTypeName.GENERIC:
            if not self.config.field.generic:
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
            assert field_text is not None
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
            assert field_text is not None
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
            assert field_text is not None
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
            assert field_text is not None
            (_, text) = field_text
            hydrated.extracted = hydration_models.FieldExtractedData(text=text)

        return hydrated

    async def _hydrate_paragraph(self, resource: Resource, paragraph_id: ParagraphId):
        if paragraph_id.full() in self.hydrated.paragraphs:
            hydrated = self.hydrated.paragraphs[paragraph_id.full()]
        else:
            hydrated = self.hydrated.add_paragraph(paragraph_id)

        if not self.config.paragraph:
            return hydrated

        config = self.config.paragraph

        field_id = paragraph_id.field_id
        field = await resource.get_field(field_id.key, field_id.pb_type)

        field_metadata = await field.get_field_metadata()
        if field_metadata is None:
            # we can't hydrate paragraphs for fields that don't have extracted
            # metadata, as we don't have any information about paragraphs
            return

        if config.text:
            text = await paragraphs.get_paragraph_text(kbid=self.kbid, paragraph_id=paragraph_id)
            hydrated.text = text

        if field_id not in self.field_paragraphs:
            index = ParagraphIndex()
            await index.build(field_id, field_metadata)
            self.field_paragraphs[field_id] = index
        field_paragraphs = self.field_paragraphs[field_id]

        if config.related:
            hydrated.related = hydration_models.RelatedParagraphRefs()

            if config.related.neighbours:
                hydrated.related.neighbours = hydration_models.RelatedNeighbourParagraphRefs()

                if config.related.neighbours.before is not None:
                    hydrated.related.neighbours.before = []
                    if config.related.neighbours.before > 0:
                        for previous_id in field_paragraphs.n_previous(
                            paragraph_id, config.related.neighbours.before
                        ):
                            await self._hydrate_related_paragraph(ParagraphId.from_string(previous_id))
                            hydrated.related.neighbours.before.insert(0, previous_id)

                if config.related.neighbours.after is not None:
                    hydrated.related.neighbours.after = []
                    if config.related.neighbours.after > 0:
                        for next_id in field_paragraphs.n_next(
                            paragraph_id, config.related.neighbours.after
                        ):
                            await self._hydrate_related_paragraph(ParagraphId.from_string(next_id))
                            hydrated.related.neighbours.after.append(next_id)

            if config.related.parents:
                hydrated.related.parents = []
                for parent_id in field_paragraphs.parents(paragraph_id):
                    await self._hydrate_related_paragraph(ParagraphId.from_string(parent_id))
                    hydrated.related.parents.append(parent_id)

            if config.related.siblings:
                hydrated.related.siblings = []
                for sibling_id in field_paragraphs.siblings(paragraph_id):
                    await self._hydrate_related_paragraph(
                        ParagraphId.from_string(sibling_id),
                    )
                    hydrated.related.siblings.append(sibling_id)

            if config.related.replacements:
                hydrated.related.replacements = []
                for replacement_id in field_paragraphs.replacements(paragraph_id):
                    await self._hydrate_related_paragraph(
                        ParagraphId.from_string(replacement_id),
                    )
                    hydrated.related.replacements.append(replacement_id)

        paragraph = field_paragraphs.get(paragraph_id)
        if config.image:
            if hydrated.image is None:
                hydrated.image = hydration_models.HydratedParagraphImage()

            if config.image.source_image:
                source_image = paragraph.representation.reference_file
                if (
                    paragraph.kind
                    in (
                        resources_pb2.Paragraph.TypeParagraph.OCR,
                        resources_pb2.Paragraph.TypeParagraph.INCEPTION,
                    )
                    and source_image
                ):
                    # Paragraphs extracted from an image store its original image
                    # representation in the reference file. The path is incomplete
                    # though, as it's stored in the `generated` folder
                    image = await download_image(
                        self.kbid,
                        field_id,
                        f"generated/{source_image}",
                        # TODO: we assume all reference files are PNG images, but
                        # this actually depends on learning so it's a dangerous
                        # assumption. We should check it by ourselves
                        mime_type="image/png",
                    )
                    hydrated.image.source_image = image

        if config.page:
            if hydrated.page is None:
                hydrated.page = hydration_models.HydratedParagraphPage()

            if config.page.page_with_visual:
                if paragraph.page.page_with_visual:
                    # Paragraphs can be found on pages with visual content. In this
                    # case, we want to return the preview of the paragraph page as
                    # an image
                    page_number = paragraph.page.page
                    preview = await download_page_preview(field, page_number)
                    if preview is not None:
                        self.hydrated.add_page_preview(paragraph_id, page_number, preview)

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
                    preview = await download_page_preview(field, page_number)
                    if preview is not None:
                        self.hydrated.add_table_page_preview(paragraph_id, page_number, preview)

        return hydrated

    async def _hydrate_related_paragraph(
        self, paragraph_id: ParagraphId
    ) -> hydration_models.HydratedParagraph:
        """Hydrate a related paragraph. This by default will only hydrate
        certain fields except if they are disabled in the paragraph hydration
        configuration.

        """
        hydrated = self.hydrated.paragraphs.get(paragraph_id.full(), None)
        if hydrated is not None:
            # this was already hydrated
            return hydrated

        assert self.config.paragraph is not None, "should be checked before reaching this function"

        hydrated = self.hydrated.add_paragraph(paragraph_id)

        if self.config.paragraph.text:
            text = await paragraphs.get_paragraph_text(kbid=self.kbid, paragraph_id=paragraph_id)
            hydrated.text = text

        return hydrated


async def download_image(
    kbid: str, field_id: FieldId, image_path: str, *, mime_type: str
) -> Optional[Image]:
    storage = await get_storage(service_name=SERVICE_NAME)
    sf = storage.file_extracted(
        kbid,
        field_id.rid,
        field_id.type,
        field_id.key,
        image_path,
    )
    raw_image = (await storage.downloadbytes(sf.bucket, sf.key)).getvalue()
    if not raw_image:
        return None
    return Image(content_type=mime_type, b64encoded=base64.b64encode(raw_image).decode())


async def download_page_preview(field: Field, page: int) -> Optional[Image]:
    """Download a specific page preview for a field and return it as an Image.
    As not all fields have previews, this function can return None.

    Page previews are uploaded by learning and shared through a known path with.
    nucliadb

    """
    field_type = FIELD_TYPE_STR_TO_NAME[field.type]

    if field_type == FieldTypeName.FILE:
        field = cast(File, field)
        metadata = await field.get_file_extracted_data()

        if metadata is None:
            return None

        assert page <= len(metadata.file_pages_previews.positions), (
            f"paragraph page number {page} should be less or equal to the total file pages previews {len(metadata.file_pages_previews.positions)}"
        )
        image = await download_image(
            field.kbid,
            field.field_id,
            f"generated/extracted_images_{page}.png",
            mime_type="image/png",
        )

    elif field_type == FieldTypeName.LINK:
        # TODO: in case of links, we want to return the link preview, that is a
        # link converted to PDF and screenshotted
        image = None

    elif (
        field_type == FieldTypeName.TEXT
        or field_type == FieldTypeName.CONVERSATION
        or field_type == FieldTypeName.GENERIC
    ):
        # these fields don't have previews
        image = None

    else:  # pragma: no cover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    return image


class ParagraphIndex:
    """Small helper class to cache field paragraphs and its relations and be
    used as an index.

    """

    NEXT = "next"
    PREVIOUS = "previous"
    PARENTS = "parents"
    SIBLINGS = "siblings"
    REPLACEMENTS = "replacements"

    def __init__(self) -> None:
        self.paragraphs: dict[str, resources_pb2.Paragraph] = {}
        self.neighbours: dict[tuple[str, str], str] = {}
        self.related: dict[tuple[str, str], list[str]] = {}

    async def build(self, field_id: FieldId, field_metadata: FieldComputedMetadata):
        self.paragraphs.clear()
        self.neighbours.clear()
        self.related.clear()

        if field_id.subfield_id is None:
            field_paragraphs = field_metadata.metadata.paragraphs
        else:
            field_paragraphs = field_metadata.split_metadata[field_id.subfield_id].paragraphs

        previous = None
        for paragraph in field_paragraphs:
            paragraph_id = field_id.paragraph_id(paragraph.start, paragraph.end).full()
            self.paragraphs[paragraph_id] = paragraph

            if previous is not None:
                self.neighbours[(previous, ParagraphIndex.NEXT)] = paragraph_id
                self.neighbours[(paragraph_id, ParagraphIndex.PREVIOUS)] = previous
            previous = paragraph_id

            self.related[(paragraph_id, ParagraphIndex.PARENTS)] = [
                parent for parent in paragraph.relations.parents
            ]
            self.related[(paragraph_id, ParagraphIndex.SIBLINGS)] = [
                sibling for sibling in paragraph.relations.siblings
            ]
            self.related[(paragraph_id, ParagraphIndex.REPLACEMENTS)] = [
                replacement for replacement in paragraph.relations.replacements
            ]

    def get(self, paragraph_id: Union[str, ParagraphId]) -> resources_pb2.Paragraph:
        paragraph_id = str(paragraph_id)
        return self.paragraphs[paragraph_id]

    def previous(self, paragraph_id: Union[str, ParagraphId]) -> Optional[str]:
        paragraph_id = str(paragraph_id)
        return self.neighbours.get((paragraph_id, ParagraphIndex.PREVIOUS))

    def next(self, paragraph_id: Union[str, ParagraphId]) -> Optional[str]:
        paragraph_id = str(paragraph_id)
        return self.neighbours.get((paragraph_id, ParagraphIndex.NEXT))

    def n_previous(self, paragraph_id: Union[str, ParagraphId], count: int = 1) -> list[str]:
        assert count >= 1, f"can't find negative previous {count}"
        paragraph_id = str(paragraph_id)
        previous: list[str] = []
        current_id = paragraph_id
        for _ in range(count):
            previous_id = self.previous(current_id)
            if previous_id is None:
                # we've reached the first paragraph
                break
            previous.insert(0, previous_id)
            current_id = previous_id
        return previous

    def n_next(self, paragraph_id: Union[str, ParagraphId], count: int = 1) -> list[str]:
        assert count >= 1, f"can't find negative nexts {count}"
        paragraph_id = str(paragraph_id)
        nexts = []
        current_id = paragraph_id
        for _ in range(count):
            next_id = self.next(current_id)
            if next_id is None:
                # we've reached the last paragraph
                break
            current_id = next_id
            nexts.append(next_id)
        return nexts

    def parents(self, paragraph_id: Union[str, ParagraphId]) -> list[str]:
        paragraph_id = str(paragraph_id)
        return self.related.get((paragraph_id, ParagraphIndex.PARENTS), [])

    def siblings(self, paragraph_id: Union[str, ParagraphId]) -> list[str]:
        paragraph_id = str(paragraph_id)
        return self.related.get((paragraph_id, ParagraphIndex.SIBLINGS), [])

    def replacements(self, paragraph_id: Union[str, ParagraphId]) -> list[str]:
        paragraph_id = str(paragraph_id)
        return self.related.get((paragraph_id, ParagraphIndex.REPLACEMENTS), [])
