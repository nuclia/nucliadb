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
import asyncio
from collections.abc import Awaitable

from async_lru import alru_cache
from fastapi import Request, Response
from fastapi_versioning import version

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB, FieldId, ParagraphId
from nucliadb.ingest.fields.base import Field
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search import cache
from nucliadb.search.search.cache import request_caches
from nucliadb.search.search.hydrator.fields import hydrate_field, page_preview_id
from nucliadb.search.search.hydrator.images import (
    download_page_preview,
)
from nucliadb.search.search.hydrator.paragraphs import ParagraphIndex, hydrate_paragraph
from nucliadb.search.search.hydrator.resources import hydrate_resource
from nucliadb_models.hydration import (
    Hydrated,
    HydratedConversationField,
    HydratedFileField,
    HydratedGenericField,
    HydratedLinkField,
    HydratedParagraph,
    HydratedResource,
    HydratedTextField,
    HydrateRequest,
    Hydration,
    ParagraphHydration,
)
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import Image
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
        self._resources: dict[str, HydratedResource] = {}
        self._fields: dict[
            str,
            (
                HydratedTextField
                | HydratedFileField
                | HydratedLinkField
                | HydratedConversationField
                | HydratedGenericField
            ),
        ] = {}
        self._paragraphs: dict[str, HydratedParagraph] = {}

    @property
    def resources(self) -> dict[str, HydratedResource]:
        return self._resources

    @property
    def fields(
        self,
    ) -> dict[
        str,
        (
            HydratedTextField
            | HydratedFileField
            | HydratedLinkField
            | HydratedConversationField
            | HydratedGenericField
        ),
    ]:
        return self._fields

    @property
    def paragraphs(self) -> dict[str, HydratedParagraph]:
        return self._paragraphs

    def build(self) -> Hydrated:
        return Hydrated(
            resources=self._resources,
            fields=self._fields,
            paragraphs=self._paragraphs,
        )

    def add_resource(self, rid: str, resource: HydratedResource):
        self._resources[rid] = resource

    def add_field(
        self,
        field_id: FieldId,
        field: (
            HydratedTextField
            | HydratedFileField
            | HydratedLinkField
            | HydratedConversationField
            | HydratedGenericField
        ),
    ):
        self._fields[field_id.full()] = field

    def has_field(self, field_id: FieldId) -> bool:
        return field_id.full() in self._fields

    def add_paragraph(self, paragraph_id: ParagraphId, paragraph: HydratedParagraph):
        self._paragraphs[paragraph_id.full()] = paragraph

    def add_page_preview(self, paragraph_id: ParagraphId, page: int, image: Image):
        field_id = paragraph_id.field_id
        field = self._fields[field_id.full()]

        if not isinstance(field, HydratedFileField):
            # Other field types have no page preview concept
            return

        if field.previews is None:
            field.previews = {}

        preview_id = page_preview_id(page)
        field.previews[preview_id] = image

        paragraph = self._paragraphs[paragraph_id.full()]
        assert paragraph.page is not None, "should already be set"
        paragraph.page.page_preview_ref = preview_id

    def add_table_page_preview(self, paragraph_id: ParagraphId, page: int, image: Image):
        field_id = paragraph_id.field_id
        field = self._fields[field_id.full()]

        if not isinstance(field, HydratedFileField):
            # Other field types have no page preview concept
            return

        if field.previews is None:
            field.previews = {}

        preview_id = page_preview_id(page)
        field.previews[preview_id] = image

        paragraph = self._paragraphs[paragraph_id.full()]
        assert paragraph.table is not None, "should already be set"
        paragraph.table.page_preview_ref = preview_id


class Hydrator:
    def __init__(self, kbid: str, config: Hydration):
        self.kbid = kbid
        self.config = config
        self.hydrated = HydratedBuilder()

        # cached paragraphs per field
        self.field_paragraphs: dict[FieldId, ParagraphIndex] = {}

        self.max_ops = asyncio.Semaphore(50)

    async def hydrate(self, paragraph_ids: list[str]) -> Hydrated:
        paragraph_tasks = {}
        field_tasks = {}
        resource_tasks = {}

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

            if field_id not in self.field_paragraphs:
                field_paragraphs_index = ParagraphIndex(field_id)
                self.field_paragraphs[field_id] = field_paragraphs_index
            field_paragraphs_index = self.field_paragraphs[field_id]

            paragraph_tasks[paragraph_id] = asyncio.create_task(
                self._limited_concurrency(
                    hydrate_paragraph(
                        resource, field, paragraph_id, self.config.paragraph, field_paragraphs_index
                    ),
                )
            )

            if field_id not in field_tasks:
                field_tasks[field_id] = asyncio.create_task(
                    self._limited_concurrency(hydrate_field(field, field_id, self.config.field))
                )

            if rid not in resource_tasks:
                if self.config.resource is not None:
                    resource_tasks[rid] = asyncio.create_task(
                        self._limited_concurrency(hydrate_resource(resource, rid, self.config.resource))
                    )

        ops = [
            *paragraph_tasks.values(),
            *field_tasks.values(),
            *resource_tasks.values(),
        ]
        results = await asyncio.gather(*ops)
        hydrated_paragraphs = results[: len(paragraph_tasks)]
        hydrated_fields = results[len(paragraph_tasks) : len(paragraph_tasks) + len(field_tasks)]
        hydrated_resources = results[
            len(paragraph_tasks) + len(field_tasks) : len(paragraph_tasks)
            + len(field_tasks)
            + len(resource_tasks)
        ]

        for rid, hydrated_resource in zip(resource_tasks.keys(), hydrated_resources):
            self.hydrated.add_resource(rid, hydrated_resource)

        for field_id, hydrated_field in zip(field_tasks.keys(), hydrated_fields):
            if hydrated_field is not None:
                self.hydrated.add_field(field_id, hydrated_field)

        for paragraph_id, (hydrated_paragraph, extra) in zip(
            paragraph_tasks.keys(), hydrated_paragraphs
        ):
            self.hydrated.add_paragraph(paragraph_id, hydrated_paragraph)

            for related_paragraph_id in extra.related_paragraph_ids:
                field_id = related_paragraph_id.field_id
                rid = related_paragraph_id.rid

                resource = await cache.get_resource(self.kbid, rid)
                if resource is None:
                    # skip resources that aren't in the DB
                    continue

                field_type_pb = FIELD_TYPE_STR_TO_PB[field_id.type]
                if not (await resource.field_exists(field_type_pb, field_id.key)):
                    # skip a fields that aren't in the DB
                    continue
                field = await resource.get_field(field_id.key, field_id.pb_type)

                if field_id not in self.field_paragraphs:
                    field_paragraphs_index = ParagraphIndex(field_id)
                    self.field_paragraphs[field_id] = field_paragraphs_index
                field_paragraphs_index = self.field_paragraphs[field_id]

                (hydrated_paragraph, _) = await hydrate_paragraph(
                    resource,
                    field,
                    related_paragraph_id,
                    ParagraphHydration(
                        text=self.config.paragraph.text, image=None, table=None, page=None, related=None
                    ),
                    field_paragraphs_index,
                )
                self.hydrated.add_paragraph(related_paragraph_id, hydrated_paragraph)

            if self.hydrated.has_field(field_id):
                # we only hydrate page and table previews for fields the user
                # allowed hydration, skipping fields with explicitly disabled
                # hydration

                if extra.field_page is not None:
                    page_number = extra.field_page
                    preview = await self.cached_download_page_preview(field, page_number)
                    if preview is not None:
                        self.hydrated.add_page_preview(paragraph_id, page_number, preview)

                if extra.field_table_page is not None:
                    page_number = extra.field_table_page
                    preview = await self.cached_download_page_preview(field, page_number)
                    if preview is not None:
                        self.hydrated.add_table_page_preview(paragraph_id, page_number, preview)

        return self.hydrated.build()

    # TODO: proper typing
    async def _limited_concurrency(self, aw: Awaitable):
        async with self.max_ops:
            return await aw

    @alru_cache(maxsize=50)
    async def cached_download_page_preview(self, field: Field, page: int) -> Image | None:
        return await download_page_preview(field, page)
