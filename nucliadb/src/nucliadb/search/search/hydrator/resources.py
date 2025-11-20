#!/usr/bin/env python3

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

from nucliadb.ingest.orm.resource import Resource
from nucliadb.models.internal.augment import (
    ResourceOrigin,
    ResourceProp,
    ResourceSecurity,
    ResourceSummary,
    ResourceTitle,
)
from nucliadb.search.augmentor.resources import db_augment_resource
from nucliadb_models import hydration as hydration_models


async def hydrate_resource(
    resource: Resource, rid: str, config: hydration_models.ResourceHydration
) -> hydration_models.HydratedResource:
    basic = await resource.get_basic()

    slug = basic.slug
    hydrated = hydration_models.HydratedResource(id=rid, slug=slug)

    select: list[ResourceProp] = []
    if config.title:
        select.append(ResourceTitle())
    if config.summary:
        select.append(ResourceSummary())
    if config.origin:
        select.append(ResourceOrigin())
    if config.security:
        select.append(ResourceSecurity())

    augmented = await db_augment_resource(resource, select)

    hydrated.title = augmented.title
    hydrated.summary = augmented.summary
    hydrated.origin = augmented.origin
    hydrated.security = augmented.security

    return hydrated
