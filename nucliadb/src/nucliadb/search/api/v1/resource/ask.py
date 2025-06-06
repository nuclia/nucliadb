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
from typing import Union

from fastapi import Header, Request, Response
from fastapi_versioning import version
from starlette.responses import StreamingResponse

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.resource.utils import get_resource_uuid_by_slug
from nucliadb.search.api.v1.router import KB_PREFIX, RESOURCE_SLUG_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import AskRequest, NucliaDBClientType, SyncAskResponse
from nucliadb_utils.authentication import requires

from ..ask import create_ask_response


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/resource/{{rid}}/ask",
    status_code=200,
    summary="Ask a resource (by id)",
    description="Ask questions to a resource",
    tags=["Search"],
    response_model=SyncAskResponse,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def resource_ask_endpoint_by_uuid(
    request: Request,
    kbid: str,
    rid: str,
    item: AskRequest,
    x_show_consumption: bool = Header(default=False),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
    x_synchronous: bool = Header(
        False,
        description="When set to true, outputs response as JSON in a non-streaming way. "
        "This is slower and requires waiting for entire answer to be ready.",
    ),
) -> Union[StreamingResponse, HTTPClientError, Response]:
    return await create_ask_response(
        kbid=kbid,
        ask_request=item,
        user_id=x_nucliadb_user,
        client_type=x_ndb_client,
        origin=x_forwarded_for,
        x_synchronous=x_synchronous,
        resource=rid,
        extra_predict_headers={"X-Show-Consumption": str(x_show_consumption).lower()},
    )


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_SLUG_PREFIX}/{{slug}}/ask",
    status_code=200,
    summary="Ask a resource (by slug)",
    description="Ask questions to a resource",
    tags=["Search"],
    response_model=SyncAskResponse,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def resource_ask_endpoint_by_slug(
    request: Request,
    kbid: str,
    slug: str,
    item: AskRequest,
    x_show_consumption: bool = Header(default=False),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
    x_synchronous: bool = Header(
        False,
        description="When set to true, outputs response as JSON in a non-streaming way. "
        "This is slower and requires waiting for entire answer to be ready.",
    ),
) -> Union[StreamingResponse, HTTPClientError, Response]:
    resource_id = await get_resource_uuid_by_slug(kbid, slug)
    if resource_id is None:
        return HTTPClientError(status_code=404, detail="Resource not found")
    return await create_ask_response(
        kbid=kbid,
        ask_request=item,
        user_id=x_nucliadb_user,
        client_type=x_ndb_client,
        origin=x_forwarded_for,
        x_synchronous=x_synchronous,
        resource=resource_id,
        extra_predict_headers={"X-Show-Consumption": str(x_show_consumption).lower()},
    )
