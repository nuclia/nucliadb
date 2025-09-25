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


from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.orm.resource import Resource
from nucliadb_models import hydration as hydration_models
from nucliadb_models.security import ResourceSecurity


async def hydrate_resource(
    resource: Resource, rid: str, config: hydration_models.ResourceHydration
) -> hydration_models.HydratedResource:
    basic = await resource.get_basic()

    slug = basic.slug
    hydrated = hydration_models.HydratedResource(id=rid, slug=slug)

    if config.title:
        hydrated.title = basic.title
    if config.summary:
        hydrated.summary = basic.summary

    if config.security:
        security = await resource.get_security()
        hydrated.security = ResourceSecurity(access_groups=[])
        if security is not None:
            for group_id in security.access_groups:
                hydrated.security.access_groups.append(group_id)

    if config.origin:
        origin = await resource.get_origin()
        if origin is not None:
            # TODO: we want a better hydration than proto to JSON
            hydrated.origin = from_proto.origin(origin)

    return hydrated
