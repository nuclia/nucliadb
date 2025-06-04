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
import logging
from contextlib import AsyncExitStack
from typing import Optional

from pydantic import BaseModel

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.serialize import managed_serialize
from nucliadb.search.search import cache, paragraphs
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import (
    FindParagraph,
    NeighbouringParagraphs,
    ResourceProperties,
)
from nucliadb_protos import resources_pb2 as rpb2
from nucliadb_telemetry.metrics import Observer
from nucliadb_utils import const
from nucliadb_utils.asyncio_utils import ConcurrentRunner
from nucliadb_utils.utilities import has_feature

logger = logging.getLogger(__name__)

hydrator_observer = Observer("hydrator", labels={"type": ""})


class ResourceHydrationOptions(BaseModel):
    """
    Options for hydrating resources.
    """

    show: list[ResourceProperties] = []
    extracted: list[ExtractedDataTypeName] = []
    field_type_filter: list[FieldTypeName] = []


class TextBlockHydrationOptions(BaseModel):
    """
    Options for hydrating text blocks (aka paragraphs).
    """

    # whether to highlight the text block with `<mark>...</mark>` tags or not
    highlight: bool = False

    # list of exact matches to highlight
    ematches: Optional[list[str]] = None

    # If true, only hydrate the text block if its text is not already populated
    only_hydrate_empty: bool = False

    neighbouring_paragraphs: Optional[NeighbouringParagraphs] = None


@hydrator_observer.wrap({"type": "resource_text"})
async def hydrate_resource_text(
    kbid: str, rid: str, *, max_concurrent_tasks: int
) -> list[tuple[FieldId, str]]:
    resource = await cache.get_resource(kbid, rid)
    if resource is None:  # pragma: no cover
        return []

    # Schedule the extraction of the text of each field in the resource
    async with get_driver().transaction(read_only=True) as txn:
        resource.txn = txn
        runner = ConcurrentRunner(max_tasks=max_concurrent_tasks)
        for field_type, field_key in await resource.get_fields(force=True):
            field_id = FieldId.from_pb(rid, field_type, field_key)
            runner.schedule(hydrate_field_text(kbid, field_id))

        # Include the summary aswell
        runner.schedule(hydrate_field_text(kbid, FieldId(rid=rid, type="a", key="summary")))

        # Wait for the results
        field_extracted_texts = await runner.wait()

    return [text for text in field_extracted_texts if text is not None]


@hydrator_observer.wrap({"type": "resource_metadata"})
async def hydrate_resource_metadata(
    kbid: str,
    resource_id: str,
    options: ResourceHydrationOptions,
    *,
    concurrency_control: Optional[asyncio.Semaphore] = None,
    service_name: Optional[str] = None,
) -> Optional[Resource]:
    """Fetch resource metadata and return it serialized."""
    show = options.show
    extracted = options.extracted

    if ResourceProperties.EXTRACTED in show and has_feature(
        const.Features.IGNORE_EXTRACTED_IN_SEARCH, context={"kbid": kbid}, default=False
    ):
        # Returning extracted metadata in search results is deprecated and this flag
        # will be set to True for all KBs in the future.
        show.remove(ResourceProperties.EXTRACTED)
        extracted = []

    async with AsyncExitStack() as stack:
        if concurrency_control is not None:
            await stack.enter_async_context(concurrency_control)

        async with get_driver().transaction(read_only=True) as ro_txn:
            serialized_resource = await managed_serialize(
                txn=ro_txn,
                kbid=kbid,
                rid=resource_id,
                show=show,
                field_type_filter=options.field_type_filter,
                extracted=extracted,
                service_name=service_name,
            )
            if serialized_resource is None:
                logger.warning(
                    "Resource not found in database", extra={"kbid": kbid, "rid": resource_id}
                )
    return serialized_resource


@hydrator_observer.wrap({"type": "field_text"})
async def hydrate_field_text(
    kbid: str,
    field_id: FieldId,
) -> Optional[tuple[FieldId, str]]:
    extracted_text_pb = await cache.get_extracted_text_from_field_id(kbid, field_id)
    if extracted_text_pb is None:  # pragma: no cover
        return None

    if field_id.subfield_id:
        return field_id, extracted_text_pb.split_text[field_id.subfield_id]
    else:
        return field_id, extracted_text_pb.text


@hydrator_observer.wrap({"type": "text_block"})
async def hydrate_text_block(
    kbid: str,
    text_block: TextBlockMatch,
    options: TextBlockHydrationOptions,
    *,
    concurrency_control: Optional[asyncio.Semaphore] = None,
) -> TextBlockMatch:
    """Given a `text_block`, fetch its corresponding text, modify and return the
    `text_block` object.

    """
    if options.only_hydrate_empty and text_block.text:
        return text_block
    async with AsyncExitStack() as stack:
        if concurrency_control is not None:
            await stack.enter_async_context(concurrency_control)
        if options.neighbouring_paragraphs is not None:
            extended_paragraph_id = await get_extended_paragraph_id(
                kbid, text_block.paragraph_id, options.neighbouring_paragraphs
            )
            text_block.paragraph_id = extended_paragraph_id
        text_block.text = await paragraphs.get_paragraph_text(
            kbid=kbid,
            paragraph_id=text_block.paragraph_id,
            highlight=options.highlight,
            matches=[],  # TODO: this was never implemented
            ematches=options.ematches,
        )
    return text_block


async def get_field_paragraphs_list(
    kbid: str,
    field: FieldId,
) -> list[ParagraphId]:
    """
    Modifies the paragraphs list by adding the paragraph ids of the field, sorted by position.
    """
    resource = await cache.get_resource(kbid, field.rid)
    if resource is None:  # pragma: no cover
        return []
    field_obj: Field = await resource.get_field(key=field.key, type=field.pb_type, load=False)
    field_metadata: Optional[rpb2.FieldComputedMetadata] = await field_obj.get_field_metadata()
    if field_metadata is None:  # pragma: no cover
        return []
    return [
        ParagraphId(
            field_id=field,
            paragraph_start=paragraph.start,
            paragraph_end=paragraph.end,
        )
        for paragraph in field_metadata.metadata.paragraphs
    ]


async def get_extended_paragraph_id(
    kbid: str,
    paragraph_id: ParagraphId,
    neighbouring_paragraphs: NeighbouringParagraphs,
) -> ParagraphId:
    field_paragraphs = await get_field_paragraphs_list(kbid, paragraph_id.field_id)
    try:
        position = field_paragraphs.index(paragraph_id)
        lb_position = max(0, position - neighbouring_paragraphs.before)
        ub_position = min(len(field_paragraphs) - 1, position + neighbouring_paragraphs.after)
        lb_pid = field_paragraphs[lb_position]
        ub_pid = field_paragraphs[ub_position]
    except IndexError:
        # Do not extend the paragraph id if it can't be found on field paragraphs
        logger.warning(
            f"Paragraph not found in field extracted paragraphs.",
            extra={
                "kbid": kbid,
                "paragraph_id": paragraph_id.full(),
            },
        )
        return paragraph_id
    return ParagraphId(
        field_id=paragraph_id.field_id,
        paragraph_start=lb_pid.paragraph_start,
        paragraph_end=ub_pid.paragraph_end,
    )


def text_block_to_find_paragraph(text_block: TextBlockMatch) -> FindParagraph:
    return FindParagraph(
        id=text_block.paragraph_id.full(),
        text=text_block.text or "",
        score=text_block.score,
        score_type=text_block.score_type,
        order=text_block.order,
        labels=text_block.paragraph_labels,
        fuzzy_result=text_block.fuzzy_search,
        is_a_table=text_block.is_a_table,
        reference=text_block.representation_file,
        page_with_visual=text_block.page_with_visual,
        position=text_block.position,
        relevant_relations=text_block.relevant_relations,
    )
