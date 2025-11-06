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

from nucliadb.search.augmentor.models import AugmentedResource
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search.hydrator import ResourceHydrationOptions, hydrate_resource_metadata


async def augment_resources(
    kbid: str,
    given: list[str],
    # select: list[ResourceProp],
    opts: ResourceHydrationOptions,
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> dict[str, AugmentedResource | None]:
    """Augment a list of resources following an augmentation"""

    ops = []
    for rid in given:
        task = asyncio.create_task(
            limited_concurrency(
                augment_resource(kbid, rid, opts),
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
    # select: list[ResourceProp],
    opts: ResourceHydrationOptions,
) -> AugmentedResource | None:
    # options = ResourceHydrationOptions()
    # for prop in select:
    #     if isinstance(prop, ResourceOrigin):
    #         options.show.append(ResourceProperties.ORIGIN)
    #     else:
    #         raise NotImplementedError(f"resource property not implemented: {prop}")

    # XXX: for now, we delegate hydration, but we augmentor should take ownership
    augmented = await hydrate_resource_metadata(kbid, rid, opts)
    return augmented
