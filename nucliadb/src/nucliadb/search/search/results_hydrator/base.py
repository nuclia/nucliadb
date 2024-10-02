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
from typing import Optional

from pydantic import BaseModel

from nucliadb.common.external_index_providers.base import QueryResults as ExternalIndexQueryResults
from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.serialize import managed_serialize
from nucliadb.search.search import paragraphs
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName
from nucliadb_models.search import (
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
    ResourceProperties,
)
from nucliadb_telemetry.metrics import Observer

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

    pass


@hydrator_observer.wrap({"type": "hydrate_external"})
async def hydrate_external(
    retrieval_results: KnowledgeboxFindResults,
    query_results: ExternalIndexQueryResults,
    kbid: str,
    resource_options: ResourceHydrationOptions = ResourceHydrationOptions(),
    text_block_options: TextBlockHydrationOptions = TextBlockHydrationOptions(),
    text_block_min_score: Optional[float] = None,
    max_parallel_operations: int = 50,
) -> None:
    """
    Hydrates the results of an external index retrieval. This includes fetching the text for the text blocks
    and the metadata for the resources.

    Parameters:
    - retrieval_results: The results of the retrieval to be hydrated.
    - query_results: The results of the query to the external index.
    - kbid: The knowledge base id.
    - resource_options: Options for hydrating resources.
    - text_block_options: Options for hydrating text blocks.
    - max_parallel_operations: The maximum number of hydration parallel operations to perform.
    """
    hydrate_ops = []
    semaphore = asyncio.Semaphore(max_parallel_operations)
    resource_ids = set()
    for text_block in query_results.iter_matching_text_blocks():
        if (
            text_block_min_score is not None and text_block.score < text_block_min_score
        ):  # pragma: no cover
            # Ignore text blocks with a score lower than the minimum
            continue
        resource_id = text_block.paragraph_id.rid
        resource_ids.add(resource_id)
        find_resource = retrieval_results.resources.setdefault(
            resource_id, FindResource(id=resource_id, fields={})
        )
        field_id = text_block.paragraph_id.field_id.full()
        find_field = find_resource.fields.setdefault(field_id, FindField(paragraphs={}))

        async def _hydrate_text_block(**kwargs):
            async with semaphore:
                await hydrate_text_block(**kwargs)

        hydrate_ops.append(
            asyncio.create_task(
                _hydrate_text_block(
                    kbid=kbid,
                    text_block=text_block,
                    options=text_block_options,
                    field_paragraphs=find_field.paragraphs,
                )
            )
        )

    async def _hydrate_resource_metadata(**kwargs):
        async with semaphore:
            await hydrate_resource_metadata(**kwargs)

    if len(resource_ids) > 0:
        async with get_driver().transaction(read_only=True) as ro_txn:
            for resource_id in resource_ids:
                hydrate_ops.append(
                    asyncio.create_task(
                        _hydrate_resource_metadata(
                            txn=ro_txn,
                            kbid=kbid,
                            resource_id=resource_id,
                            options=resource_options,
                            find_resources=retrieval_results.resources,
                        )
                    )
                )

    if len(hydrate_ops) > 0:
        await asyncio.gather(*hydrate_ops)


@hydrator_observer.wrap({"type": "text_block"})
async def hydrate_text_block(
    kbid: str,
    text_block: TextBlockMatch,
    options: TextBlockHydrationOptions,
    field_paragraphs: dict[str, FindParagraph],
) -> None:
    """
    Fetch the text for a text block and update the FindParagraph object.
    """
    text = await paragraphs.get_paragraph_text(kbid=kbid, paragraph_id=text_block.paragraph_id)
    text_block_id = text_block.paragraph_id.full()
    field_paragraphs[text_block_id] = FindParagraph(
        score=text_block.score,
        score_type=text_block.score_type,
        order=text_block.order,
        text=text,
        id=text_block_id,
        labels=text_block.paragraph_labels,
        fuzzy_result=False,
        is_a_table=text_block.is_a_table,
        reference=text_block.representation_file,
        page_with_visual=text_block.page_with_visual,
        position=text_block.position,
    )


@hydrator_observer.wrap({"type": "resource_metadata"})
async def hydrate_resource_metadata(
    txn: Transaction,
    kbid: str,
    resource_id: str,
    options: ResourceHydrationOptions,
    find_resources: dict[str, FindResource],
) -> None:
    """
    Fetch the various metadata fields of the resource and update the FindResource object.
    """
    serialized_resource = await managed_serialize(
        txn=txn,
        kbid=kbid,
        rid=resource_id,
        show=options.show,
        field_type_filter=options.field_type_filter,
        extracted=options.extracted,
    )
    if serialized_resource is None:
        logger.warning(
            "Resource not found in database",
            extra={
                "kbid": kbid,
                "rid": resource_id,
            },
        )
        find_resources.pop(resource_id, None)
        return
    find_resources[resource_id].updated_from(serialized_resource)


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
