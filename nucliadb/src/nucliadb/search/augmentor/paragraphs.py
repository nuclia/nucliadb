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
from collections.abc import Sequence

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB, ParagraphId
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.resource import Resource
from nucliadb.models.internal.augment import (
    AugmentedParagraph,
    AugmentedRelatedParagraphs,
    Metadata,
    Paragraph,
    ParagraphImage,
    ParagraphPage,
    ParagraphProp,
    ParagraphTable,
    ParagraphText,
    RelatedParagraphs,
)
from nucliadb.search import logger
from nucliadb.search.augmentor.metrics import augmentor_observer
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search import cache
from nucliadb.search.search.paragraphs import get_paragraph_from_full_text
from nucliadb_protos import resources_pb2


async def augment_paragraphs(
    kbid: str,
    given: list[Paragraph],
    select: list[ParagraphProp],
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> dict[ParagraphId, AugmentedParagraph | None]:
    """Augment a list of paragraphs following an augmentation"""

    ops = []
    for paragraph in given:
        task = asyncio.create_task(
            limited_concurrency(
                augment_paragraph(kbid, paragraph.id, select, paragraph.metadata),
                max_ops=concurrency_control,
            )
        )
        ops.append(task)
    results: list[AugmentedParagraph | None] = await asyncio.gather(*ops)

    augmented = {}
    for paragraph, augmentation in zip(given, results):
        augmented[paragraph.id] = augmentation

    return augmented


async def augment_paragraph(
    kbid: str,
    paragraph_id: ParagraphId,
    select: list[ParagraphProp],
    metadata: Metadata | None,
) -> AugmentedParagraph | None:
    rid = paragraph_id.rid
    resource = await cache.get_resource(kbid, rid)
    if resource is None:
        # skip resources that aren't in the DB
        return None

    field_id = paragraph_id.field_id
    field_type_pb = FIELD_TYPE_STR_TO_PB[field_id.type]
    # we must check if field exists or get_field will return an empty field
    # (behaviour thought for ingestion) that we don't want
    if not (await resource.field_exists(field_type_pb, field_id.key)):
        # skip a fields that aren't in the DB
        return None
    field = await resource.get_field(field_id.key, field_id.pb_type)

    # TODO(decoupled-ask): make sure we don't repeat any select clause

    return await db_augment_paragraph(resource, field, paragraph_id, select, metadata)


async def db_augment_paragraph(
    resource: Resource,
    field: Field,
    paragraph_id: ParagraphId,
    select: list[ParagraphProp],
    metadata: Metadata | None,
) -> AugmentedParagraph:
    # we use an accessor to get the metadata to avoid unnecessary DB round
    # trips. With this, we'll only fetch it one and only if we need it
    _metadata = metadata
    _metadata_available = True

    async def access_metadata() -> Metadata | None:
        nonlocal _metadata, _metadata_available

        if _metadata is None and _metadata_available:
            _metadata = await db_paragraph_metadata(field, paragraph_id)

        if _metadata is None:
            _metadata_available = False

        return _metadata

    text = None
    image_path = None
    page_preview_path = None
    related = None
    for prop in select:
        if isinstance(prop, ParagraphText):
            text = await get_paragraph_text(field, paragraph_id)

        elif isinstance(prop, ParagraphImage):
            metadata = await access_metadata()
            if metadata is None:
                continue
            if metadata.is_an_image and metadata.source_file:
                image_path = f"generated/{metadata.source_file}"

        elif isinstance(prop, ParagraphTable):
            metadata = await access_metadata()
            if metadata is None:
                continue
            if metadata.is_a_table:
                if prop.prefer_page_preview and metadata.page and metadata.in_page_with_visual:
                    page_preview_path = f"generated/extracted_images_{metadata.page}.png"
                elif metadata.source_file:
                    image_path = f"generated/{metadata.source_file}"

        elif isinstance(prop, ParagraphPage):
            if prop.preview:
                metadata = await access_metadata()
                if metadata is None:
                    continue
                if metadata.page and metadata.in_page_with_visual:
                    page_preview_path = f"generated/extracted_images_{metadata.page}.png"

        elif isinstance(prop, RelatedParagraphs):
            related = await related_paragraphs(
                field,
                paragraph_id,
                neighbours_before=prop.neighbours_before,
                neighbours_after=prop.neighbours_after,
            )

        else:  # pragma: no cover
            logger.warning(f"Unexpected paragraph prop: {prop}")

    return AugmentedParagraph(
        id=paragraph_id,
        text=text,
        source_image_path=image_path,
        page_preview_path=page_preview_path,
        related=related,
    )


async def db_paragraph_metadata(field: Field, paragraph_id: ParagraphId) -> Metadata | None:
    """Obtain paragraph metadata from the source of truth (maindb/blob).

    This operation may require data from blob storage, which makes it costly.

    """
    field_paragraphs = await get_field_paragraphs(field)
    if field_paragraphs is None:
        # We don't have paragraph metadata for this field, we can't do anything
        return None

    for paragraph in field_paragraphs:
        field_paragraph_id = field.field_id.paragraph_id(paragraph.start, paragraph.end)
        if field_paragraph_id == paragraph_id:
            metadata = Metadata.from_db_paragraph(paragraph)
            return metadata
    else:
        return None


async def get_field_paragraphs(field: Field) -> Sequence[resources_pb2.Paragraph] | None:
    field_metadata = await field.get_field_metadata()
    if field_metadata is None:
        return None

    field_id = field.field_id
    if field_id.subfield_id is None:
        field_paragraphs = field_metadata.metadata.paragraphs
    else:
        field_paragraphs = field_metadata.split_metadata[field_id.subfield_id].paragraphs

    return field_paragraphs


@augmentor_observer.wrap({"type": "paragraph_text"})
async def get_paragraph_text(field: Field, paragraph_id: ParagraphId) -> str | None:
    text = await get_paragraph_from_full_text(
        field=field,
        start=paragraph_id.paragraph_start,
        end=paragraph_id.paragraph_end,
        split=paragraph_id.field_id.subfield_id,
        log_on_missing_field=True,
    )
    # we want to be explicit with not having the paragraph text but the function
    # above returns an empty string if it can't find it
    return text or None


async def related_paragraphs(
    field: Field,
    paragraph_id: ParagraphId,
    *,
    neighbours_before: int = 0,
    neighbours_after: int = 0,
) -> AugmentedRelatedParagraphs | None:
    field_paragraphs = await get_field_paragraphs(field)
    if field_paragraphs is None:
        return None

    idx: int | None
    for idx, paragraph in enumerate(field_paragraphs):
        field_paragraph_id = field.field_id.paragraph_id(paragraph.start, paragraph.end)
        if field_paragraph_id == paragraph_id:
            break
    else:
        # we haven't found the paragraph, we won't find any related either
        return None

    before = []
    for idx_before in range(max(idx - neighbours_before, 0), idx):
        paragraph = field_paragraphs[idx_before]
        paragraph_id = field.field_id.paragraph_id(paragraph.start, paragraph.end)
        before.append(paragraph_id)

    after = []
    for idx_after in range(idx + 1, min(idx + 1 + neighbours_after, len(field_paragraphs))):
        paragraph = field_paragraphs[idx_after]
        paragraph_id = field.field_id.paragraph_id(paragraph.start, paragraph.end)
        after.append(paragraph_id)

    return AugmentedRelatedParagraphs(
        neighbours_before=before,
        neighbours_after=after,
    )
