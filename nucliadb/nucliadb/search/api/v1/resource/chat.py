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
from nucliadb.search.api.v1.find import find
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.predict import SendToPredictError
from nucliadb.search.search.chat.query import chat, rephrase_query_from_context
from nucliadb.search.search.exceptions import IncompleteFindResultsError
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    ChatRequest,
    FindRequest,
    NucliaDBClientType,
    SearchOptions,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/resource/{{rid}}/chat",
    status_code=200,
    name="Chat with a Resource (by id)",
    summary="Chat with a resource",
    description="Chat with a resource",
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def resource_chat_endpoint(
    request: Request,
    response: Response,
    kbid: str,
    rid: str,
    item: ChatRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[StreamingResponse, HTTPClientError]:
    try:
        return await resource_chat(
            response, kbid, rid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
        )
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
    except SendToPredictError:
        return HTTPClientError(status_code=503, detail="Chat service not available")
    except IncompleteFindResultsError:
        return HTTPClientError(
            status_code=529,
            detail="Temporary error on information retrieval. Please try again.",
        )


async def resource_chat(
    response: Response,
    kbid: str,
    rid: str,
    item: ChatRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
):
    user_query = item.query
    rephrased_query = None
    if item.context and len(item.context) > 0:
        rephrased_query = await rephrase_query_from_context(
            kbid, item.context, item.query, x_nucliadb_user
        )

    find_request = FindRequest()
    find_request.resource_filters = [rid]
    find_request.features = [
        SearchOptions.PARAGRAPH,
        SearchOptions.VECTOR,
    ]
    find_request.query = rephrased_query or user_query
    find_request.fields = item.fields
    find_request.filters = item.filters
    find_request.field_type_filter = item.field_type_filter
    find_request.min_score = item.min_score
    find_request.range_creation_start = item.range_creation_start
    find_request.range_creation_end = item.range_creation_end
    find_request.range_modification_start = item.range_modification_start
    find_request.range_modification_end = item.range_modification_end
    find_request.show = item.show
    find_request.extracted = item.extracted
    find_request.shards = item.shards
    find_request.autofilter = item.autofilter
    find_request.highlight = item.highlight

    find_results, incomplete = await find(
        response,
        kbid,
        find_request,
        x_ndb_client,
        x_nucliadb_user,
        x_forwarded_for,
        do_audit=True,
    )
    if incomplete:
        raise IncompleteFindResultsError()

    return await chat(
        kbid,
        user_query,
        rephrased_query,
        find_results,
        item,
        x_nucliadb_user,
        x_ndb_client,
        x_forwarded_for,
    )
