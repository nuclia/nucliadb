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
from fastapi import Request
from fastapi_versioning import version

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, RESOURCE_SLUG_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/resource/{{rid}}/chat",
    status_code=200,
    summary="Chat with a resource (by id)",
    description="Chat with a resource",
    tags=["Search"],
    response_model=None,
    deprecated=True,
    include_in_schema=False,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def resource_chat_endpoint_by_uuid(
    request: Request,
    kbid: str,
    rid: str,
) -> HTTPClientError:
    return HTTPClientError(
        status_code=404,
        detail="This endpoint has been removed deprecated. Please use /ask instead.",
    )


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_SLUG_PREFIX}/{{slug}}/chat",
    status_code=200,
    summary="Chat with a resource (by slug)",
    description="Chat with a resource",
    tags=["Search"],
    response_model=None,
    deprecated=True,
    include_in_schema=False,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def resource_chat_endpoint_by_slug(
    request: Request,
    kbid: str,
    slug: str,
) -> HTTPClientError:
    return HTTPClientError(
        status_code=404,
        detail=(
            "This endpoint has been removed. Use /ask instead."
            "For more information on how to migrate to use /ask, please refer to: "
            "https://docs.nuclia.dev/docs/changelog/updates/2024-05-30/#migration-guide"
        ),
    )
