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

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB, ParagraphId
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.resource import Resource
from nucliadb.models.internal.augment import (
    ParagraphImage,
    ParagraphProp,
    ParagraphTable,
    ParagraphText,
    RelatedParagraphs,
)
from nucliadb.search import logger
from nucliadb.search.augmentor.models import AugmentedParagraph, Metadata, Paragraph
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search import cache
from nucliadb.search.search.paragraphs import get_paragraph_from_full_text
from nucliadb_models.search import Image


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

    if metadata is None:
        metadata = await db_paragraph_metadata(field, paragraph_id)
        if metadata is None:
            # we are unable to get any paragraph metadata, we can't continue
            return None

    return await db_augment_paragraph(resource, field, paragraph_id, select, metadata)


async def db_augment_paragraph(
    resource: Resource,
    field: Field,
    paragraph_id: ParagraphId,
    select: list[ParagraphProp],
    metadata: Metadata,
) -> AugmentedParagraph | None:
    text = None
    image = None
    for prop in select:
        if isinstance(prop, ParagraphText) and text is None:
            text = await get_paragraph_text(field, paragraph_id)

        elif isinstance(prop, ParagraphImage):
            if metadata.is_an_image and metadata.source_file:
                kbid = field.kbid
                image = await download_paragraph_source_image(kbid, paragraph_id, metadata.source_file)

            raise NotImplementedError()

        elif isinstance(prop, ParagraphTable):
            raise NotImplementedError()

        elif isinstance(prop, RelatedParagraphs):
            raise NotImplementedError()

        else:  # pragma: no cover
            logger.warning(f"Unexpected paragraph prop: {prop}")

    return AugmentedParagraph(
        id=paragraph_id,
        text=text,
        source_image=image,
    )


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


async def download_paragraph_source_image(
    kbid: str, paragraph_id: ParagraphId, source_image: str
) -> Image | None:
    from nucliadb.search.search.hydrator.images import download_image

    field_id = paragraph_id.field_id

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


async def db_paragraph_metadata(field: Field, paragraph_id: ParagraphId) -> Metadata | None:
    field_metadata = await field.get_field_metadata()
    if field_metadata is None:
        # We don't have metadata for this field and thus, for this paragraph.
        return None

    field_id = field.field_id
    if field_id.subfield_id is None:
        field_paragraphs = field_metadata.metadata.paragraphs
    else:
        field_paragraphs = field_metadata.split_metadata[field_id.subfield_id].paragraphs

    for paragraph in field_paragraphs:
        field_paragraph_id = field_id.paragraph_id(paragraph.start, paragraph.end).full()
        if field_paragraph_id == paragraph_id:
            return Metadata.from_db_paragraph(paragraph)
    else:
        return Metadata.unknown()
