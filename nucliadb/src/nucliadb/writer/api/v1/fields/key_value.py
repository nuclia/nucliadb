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

from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.writer.api.v1.field import add_field_to_resource, add_field_to_resource_by_slug
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.utils import FieldIdString
from nucliadb_models.writer import KeyValueField, ResourceFieldAdded
from nucliadb_utils.authentication import requires


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/key_value/{{field_id}}",
    status_code=201,
    summary="Add resource key-value field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_key_value_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    item: KeyValueField,
):
    return await add_field_to_resource_by_slug(request, kbid, rslug, field_id, item)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/key_value/{{field_id}}",
    status_code=201,
    summary="Add resource key-value field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_key_value_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    item: KeyValueField,
):
    return await add_field_to_resource(request, kbid, rid, field_id, item)
