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
from nucliadb.search import logger
from nucliadb.search.augmentor.models import AugmentedParagraph
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search import cache
from nucliadb.search.search.paragraphs import get_paragraph_from_full_text
from nucliadb_models.internal.augment import (
    ParagraphImage,
    ParagraphProp,
    ParagraphTable,
    ParagraphText,
    RelatedParagraphs,
)


async def augment_paragraphs(
    kbid: str,
    given: list[ParagraphId],
    select: list[ParagraphProp],
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> dict[ParagraphId, AugmentedParagraph | None]:
    """Augment a list of paragraphs following an augmentation"""

    ops = []
    for paragraph_id in given:
        task = asyncio.create_task(
            limited_concurrency(
                augment_paragraph(kbid, paragraph_id, select),
                max_ops=concurrency_control,
            )
        )
        ops.append(task)
    results: list[AugmentedParagraph | None] = await asyncio.gather(*ops)

    augmented = {}
    for paragraph_id, augmentation in zip(given, results):
        augmented[paragraph_id] = augmentation

    return augmented


async def augment_paragraph(
    kbid: str,
    paragraph_id: ParagraphId,
    select: list[ParagraphProp],
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

    return await db_augment_paragraph(resource, field, paragraph_id, select)


async def db_augment_paragraph(
    resource: Resource,
    field: Field,
    paragraph_id: ParagraphId,
    select: list[ParagraphProp],
) -> AugmentedParagraph | None:
    text = None
    for prop in select:
        if isinstance(prop, ParagraphText) and text is None:
            text = await get_paragraph_text(field, paragraph_id)
        elif isinstance(prop, ParagraphImage):
            raise NotImplementedError()
        elif isinstance(prop, ParagraphTable):
            raise NotImplementedError()
        elif isinstance(prop, RelatedParagraphs):
            raise NotImplementedError()
        else:  # pragma: no cover
            logger.warning(f"Unexpected paragraph prop: {prop}")

    return AugmentedParagraph(id=paragraph_id, text=text)


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
