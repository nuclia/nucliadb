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

from typing_extensions import assert_never

import nucliadb_models.resource
from nucliadb.common import datamanagers
from nucliadb.ingest.orm.resource import Resource
from nucliadb.ingest.serialize import (
    serialize_extra,
    serialize_origin,
    serialize_resource,
    serialize_security,
)
from nucliadb.models.internal.augment import (
    AugmentedResource,
    ResourceClassificationLabels,
    ResourceExtra,
    ResourceOrigin,
    ResourceProp,
    ResourceSecurity,
    ResourceSummary,
    ResourceTitle,
)
from nucliadb.search.augmentor.metrics import augmentor_observer
from nucliadb.search.augmentor.utils import limited_concurrency
from nucliadb.search.search import cache
from nucliadb.search.search.hydrator import ResourceHydrationOptions
from nucliadb_models.search import ResourceProperties
from nucliadb_protos import resources_pb2
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature


async def augment_resources(
    kbid: str,
    given: list[str],
    select: list[ResourceProp],
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> dict[str, AugmentedResource | None]:
    """Augment a list of resources following an augmentation"""

    ops = []
    for rid in given:
        task = asyncio.create_task(
            limited_concurrency(
                augment_resource(kbid, rid, select),
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
) -> AugmentedResource | None:
    # TODO(decoupled-ask): make sure we don't repeat any select clause

    resource = await cache.get_resource(kbid, rid)
    if resource is None:
        # skip resources that aren't in the DB
        return None

    return await db_augment_resource(resource, select)


@augmentor_observer.wrap({"type": "db_resource"})
async def db_augment_resource(
    resource: Resource,
    select: list[ResourceProp],
) -> AugmentedResource:
    title = None
    summary = None
    origin = None
    extra = None
    security = None
    labels = None

    basic = None
    for prop in select:
        if isinstance(prop, ResourceTitle):
            if basic is None:
                basic = await resource.get_basic()
            if basic is not None:
                title = basic.title

        elif isinstance(prop, ResourceSummary):
            if basic is None:
                basic = await resource.get_basic()
            if basic is not None:
                summary = basic.summary

        elif isinstance(prop, ResourceOrigin):
            # REVIEW(decoupled-ask): we may want a better hydration than proto to JSON
            origin = await serialize_origin(resource)

        elif isinstance(prop, ResourceExtra):
            extra = await serialize_extra(resource)

        elif isinstance(prop, ResourceSecurity):
            security = await serialize_security(resource)

        elif isinstance(prop, ResourceClassificationLabels):
            labels = await classification_labels(resource)

        else:
            assert_never(prop)

    augmented = AugmentedResource(
        id=resource.uuid,
        title=title,
        summary=summary,
        origin=origin,
        extra=extra,
        security=security,
        classification_labels=labels,
    )
    return augmented


async def get_basic(resource: Resource) -> resources_pb2.Basic | None:
    # HACK: resource.get_basic() always returns a pb, even if it's not in the
    # DB. Here we really want to know if there's basic or not
    basic = await datamanagers.resources.get_basic(resource.txn, kbid=resource.kbid, rid=resource.uuid)
    return basic


async def classification_labels(resource: Resource) -> dict[str, set[str]] | None:
    basic = await get_basic(resource)
    if basic is None:
        return None

    labels: dict[str, set[str]] = {}
    for classification in basic.usermetadata.classifications:
        labels.setdefault(classification.labelset, set()).add(classification.label)
    return labels


async def augment_resources_deep(
    kbid: str,
    given: list[str],
    opts: ResourceHydrationOptions,
    *,
    concurrency_control: asyncio.Semaphore | None = None,
) -> dict[str, nucliadb_models.resource.Resource | None]:
    """Augment resources using the Resource model. Depending on the options,
    this can serialize resource fields, extracted data like text, vectors...

    Thus, this operation can be quite expensive.

    """

    if ResourceProperties.EXTRACTED in opts.show and has_feature(
        const.Features.IGNORE_EXTRACTED_IN_SEARCH, context={"kbid": kbid}, default=False
    ):
        # Returning extracted metadata in search results is deprecated and this flag
        # will be set to True for all KBs in the future.
        opts.show.remove(ResourceProperties.EXTRACTED)
        opts.extracted.clear()

    ops = []
    for rid in given:
        task = asyncio.create_task(
            limited_concurrency(
                augment_resource_deep(kbid, rid, opts),
                max_ops=concurrency_control,
            )
        )
        ops.append(task)
    results: list[nucliadb_models.resource.Resource | None] = await asyncio.gather(*ops)

    augmented: dict[str, nucliadb_models.resource.Resource | None] = {}
    for rid, augmentation in zip(given, results):
        augmented[rid] = augmentation

    return augmented


@augmentor_observer.wrap({"type": "seialize_resource"})
async def augment_resource_deep(
    kbid: str,
    rid: str,
    opts: ResourceHydrationOptions,
) -> nucliadb_models.resource.Resource | None:
    """Augment a resource using the Resource model. Depending on the options,
    this can serialize resource fields, extracted data like text, vectors...

    Thus, this operation can be quite expensive.

    """
    resource = await cache.get_resource(kbid, rid)
    if resource is None:
        # skip resources that aren't in the DB
        return None

    serialized = await serialize_resource(
        resource,
        show=opts.show,
        field_type_filter=opts.field_type_filter,
        extracted=opts.extracted,
    )
    return serialized
