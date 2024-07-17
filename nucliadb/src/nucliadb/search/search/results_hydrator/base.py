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

from nucliadb.common.external_index_providers.base import QueryResults as ExternalIndexQueryResults
from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.serialize import managed_serialize
from nucliadb.search.search import paragraphs
from nucliadb.search.search.cache import get_resource_cache
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName
from nucliadb_models.search import (
    SCORE_TYPE,
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
    ResourceProperties,
    TextPosition,
)

logger = logging.getLogger(__name__)


async def hydrate_external(
    retrieval_results: KnowledgeboxFindResults,
    query_results: ExternalIndexQueryResults,
    kbid: str,
    show: list[ResourceProperties],
    extracted: list[ExtractedDataTypeName],
    field_type_filter: list[FieldTypeName],
    max_parallel_operations: int = 50,
) -> None:
    """
    Hydrates the results of an external index retrieval:
    - Text for the matched text blocks.
    - Metadata for the matched resources.
    """
    hydrate_ops = []
    semaphore = asyncio.Semaphore(max_parallel_operations)
    extracted_text_cache = paragraphs.ExtractedTextCache()
    rcache = get_resource_cache(clear=True)
    try:
        resource_ids = set()
        for text_block in query_results.iter_matching_text_blocks():
            resource_id = text_block.resource_id
            resource_ids.add(resource_id)
            find_resource = retrieval_results.resources.setdefault(
                resource_id, FindResource(id=resource_id, fields={})
            )
            find_resource.fields.setdefault(text_block.field, FindField(paragraphs={}))

            async def _hydrate_text_block(**kwargs):
                async with semaphore:
                    await hydrate_text_block(**kwargs)

            hydrate_ops.append(
                asyncio.create_task(
                    _hydrate_text_block(
                        kbid=kbid,
                        text_block=text_block,
                        extracted_text_cache=extracted_text_cache,
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
                                show=show,
                                field_type_filter=field_type_filter,
                                extracted=extracted,
                                find_resources=retrieval_results.resources,
                            )
                        )
                    )

        if len(hydrate_ops) > 0:
            await asyncio.gather(*hydrate_ops)
    finally:
        extracted_text_cache.clear()
        rcache.clear()


async def hydrate_text_block(
    kbid: str,
    text_block: TextBlockMatch,
    extracted_text_cache: paragraphs.ExtractedTextCache,
    field_paragraphs: dict[str, FindParagraph],
) -> None:
    """
    Fetch the text for a text block and update the FindParagraph object.
    """
    text = await paragraphs.get_paragraph_text(
        kbid=kbid,
        rid=text_block.resource_id,
        field=text_block.field,
        start=text_block.position_start,
        end=text_block.position_end,
        split=text_block.split,
        extracted_text_cache=extracted_text_cache,
    )
    field_paragraphs[text_block.id] = FindParagraph(
        score=text_block.score,
        score_type=SCORE_TYPE.EXTERNAL,
        text=text,
        id=text_block.id,
        labels=[],  # TODO
        fuzzy_result=False,  # TODO
        is_a_table=False,  # TODO
        reference=None,  # TODO
        page_with_visual=False,  # TODO
        position=TextPosition(
            page_number=None,  # TODO
            index=0,  # TODO
            start=text_block.position_start,
            end=text_block.position_end,
            start_seconds=None,  # TODO
            end_seconds=None,  # TODO
        ),
    )


async def hydrate_resource_metadata(
    txn: Transaction,
    kbid: str,
    resource_id: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    find_resources: dict[str, FindResource],
) -> None:
    """
    Fetch the various metadata fields of the resource and update the FindResource object.
    """
    serialized_resource = await managed_serialize(
        txn=txn,
        kbid=kbid,
        rid=resource_id,
        show=show,
        field_type_filter=field_type_filter,
        extracted=extracted,
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
