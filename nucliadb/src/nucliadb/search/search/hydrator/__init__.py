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

from nucliadb.common.ids import FieldId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.serialize import managed_serialize
from nucliadb.search.search import cache
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import ExtractedDataTypeName, Resource
from nucliadb_models.search import ResourceProperties
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


@hydrator_observer.wrap({"type": "resource_text"})
async def hydrate_resource_text(
    kbid: str, rid: str, *, max_concurrent_tasks: int
) -> list[tuple[FieldId, str]]:
    resource = await cache.get_resource(kbid, rid)
    if resource is None:  # pragma: no cover
        return []

    # Schedule the extraction of the text of each field in the resource
    async with get_driver().ro_transaction() as txn:
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

        async with get_driver().ro_transaction() as ro_txn:
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
    from nucliadb.models.internal.augment import FieldText
    from nucliadb.search.augmentor.fields import augment_field

    augmented = await augment_field(kbid, field_id, select=[FieldText()])
    if augmented is None or augmented.text is None:
        # we haven't found the resource, field or text
        return None

    return field_id, augmented.text
