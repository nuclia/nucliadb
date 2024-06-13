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
from typing import Optional, Union

from fastapi import Header, Request, Response
from fastapi_versioning import version
from starlette.responses import StreamingResponse

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search.chat.ask import AskResult, ask, handled_ask_exceptions
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    AskRequest,
    NucliaDBClientType,
    SyncAskResponse,
    parse_max_tokens,
)
from nucliadb_utils.authentication import requires


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/ask",
    status_code=200,
    summary="Ask Knowledge Box",
    description="Ask questions on a Knowledge Box",
    tags=["Search"],
    response_model=SyncAskResponse,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def ask_knowledgebox_endpoint(
    request: Request,
    kbid: str,
    item: AskRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
    x_synchronous: bool = Header(
        default=False,
        description="When set to true, outputs response as JSON in a non-streaming way. "
        "This is slower and requires waiting for entire answer to be ready.",
    ),
) -> Union[StreamingResponse, HTTPClientError, Response]:
    return await create_ask_response(
        kbid, item, x_nucliadb_user, x_ndb_client, x_forwarded_for, x_synchronous
    )


@handled_ask_exceptions
async def create_ask_response(
    kbid: str,
    ask_request: AskRequest,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    x_synchronous: bool,
    resource: Optional[str] = None,
) -> Response:
    ask_request.max_tokens = parse_max_tokens(ask_request.max_tokens)
    ask_result: AskResult = await ask(
        kbid=kbid,
        ask_request=ask_request,
        user_id=user_id,
        client_type=client_type,
        origin=origin,
        resource=resource,
    )
    headers = {
        "NUCLIA-LEARNING-ID": ask_result.nuclia_learning_id or "unknown",
        "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
    }
    if x_synchronous:
        return Response(
            content=await ask_result.json(),
            status_code=200,
            headers=headers,
            media_type="application/json",
        )
    else:
        return StreamingResponse(
            content=ask_result.ndjson_stream(),
            status_code=200,
            headers=headers,
            media_type="application/x-ndjson",
        )
