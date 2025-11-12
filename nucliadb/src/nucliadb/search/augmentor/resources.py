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

from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.serialize import serialize_resource
from nucliadb.models.internal.augment import (
    ResourceExtra,
    ResourceOrigin,
    ResourceProp,
    ResourceSecurity,
)
from nucliadb.search.augmentor.metrics import augmentor_observer
from nucliadb.search.augmentor.models import AugmentedResource
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search import cache
from nucliadb.search.search.hydrator import ResourceHydrationOptions
from nucliadb_models.search import ResourceProperties
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature


async def augment_resources(
    kbid: str,
    given: list[str],
    select: list[ResourceProp],
    opts: ResourceHydrationOptions,
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> dict[str, AugmentedResource | None]:
    """Augment a list of resources following an augmentation"""

    ops = []
    for rid in given:
        task = asyncio.create_task(
            limited_concurrency(
                augment_resource(kbid, rid, select, opts),
                max_ops=concurrency_control,
            )
        )
        ops.append(task)
    results: list[AugmentedResource | None] = await asyncio.gather(*ops)

    augmented = {}
    for rid, augmentation in zip(given, results):
        augmented[rid] = augmentation

    return augmented


async def augment_resource(
    kbid: str,
    rid: str,
    select: list[ResourceProp],
    opts: ResourceHydrationOptions,
) -> AugmentedResource | None:
    # TODO: make sure we don't repeat any select clause

    resource = await cache.get_resource(kbid, rid)
    if resource is None:
        # skip resources that aren't in the DB
        return None

    return await db_augment_resource(resource, select, opts)


@augmentor_observer.wrap({"type": "db_resource"})
async def db_augment_resource(
    resource: Resource,
    select: list[ResourceProp],
    opts: ResourceHydrationOptions,
) -> AugmentedResource | None:
    kbid = resource.kb.kbid

    for prop in select:
        if isinstance(prop, ResourceOrigin):
            opts.show.append(ResourceProperties.ORIGIN)
        elif isinstance(prop, ResourceExtra):
            opts.show.append(ResourceProperties.EXTRA)
        elif isinstance(prop, ResourceSecurity):
            opts.show.append(ResourceProperties.SECURITY)
        else:
            raise NotImplementedError(f"resource property not implemented: {prop}")

    # XXX: for now, we delegate hydration, but we augmentor should take ownership

    if ResourceProperties.EXTRACTED in opts.show and has_feature(
        const.Features.IGNORE_EXTRACTED_IN_SEARCH, context={"kbid": kbid}, default=False
    ):
        # Returning extracted metadata in search results is deprecated and this flag
        # will be set to True for all KBs in the future.
        opts.show.remove(ResourceProperties.EXTRACTED)
        opts.extracted.clear()

    augmented = await serialize_resource(
        resource,
        show=opts.show,
        field_type_filter=opts.field_type_filter,
        extracted=opts.extracted,
    )
    return augmented
