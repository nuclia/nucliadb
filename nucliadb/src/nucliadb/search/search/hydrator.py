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
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.serialize import managed_serialize
from nucliadb.search.search import paragraphs
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import (
    FindParagraph,
    ResourceProperties,
)
from nucliadb_telemetry.metrics import Observer
from nucliadb_utils import const
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
    async with AsyncExitStack() as stack:
        if concurrency_control is not None:
            await stack.enter_async_context(concurrency_control)

        text_block.text = await paragraphs.get_paragraph_text(
            kbid=kbid,
            paragraph_id=text_block.paragraph_id,
            highlight=options.highlight,
            matches=[],  # TODO: this was never implemented
            ematches=options.ematches,
        )
    return text_block


@hydrator_observer.wrap({"type": "resource_metadata_simple"})
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
    )
