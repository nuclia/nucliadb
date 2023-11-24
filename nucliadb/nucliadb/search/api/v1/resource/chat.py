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
from nucliadb.search import predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.search.exceptions import (
    IncompleteFindResultsError,
    InvalidQueryError,
)
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import ChatRequest, NucliaDBClientType
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError

from ..chat import create_chat_response


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/resource/{{rid}}/chat",
    status_code=200,
    name="Chat with a Resource (by id)",
    summary="Chat with a resource",
    description="Chat with a resource",
    tags=["Search"],
    response_model=None,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def resource_chat_endpoint(
    request: Request,
    kbid: str,
    rid: str,
    item: ChatRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
    x_synchronous: bool = Header(
        False,
        description="Output response as JSON in a non-streaming way. "
        "This is slower and requires waiting for entire answer to be ready.",
    ),
) -> Union[StreamingResponse, HTTPClientError, Response]:
    try:
        item.resource_filters = [rid]
        return await create_chat_response(
            kbid, item, x_nucliadb_user, x_ndb_client, x_forwarded_for, x_synchronous
        )
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
    except predict.ProxiedPredictAPIError as err:
        return HTTPClientError(
            status_code=503,
            detail=f"Chat service unavailable. {err.status}: {err.detail}",
        )
    except IncompleteFindResultsError:
        return HTTPClientError(
            status_code=529,
            detail="Temporary error on information retrieval. Please try again.",
        )
    except predict.RephraseMissingContextError:
        return HTTPClientError(
            status_code=412,
            detail="Unable to rephrase the query with the provided context.",
        )
    except predict.RephraseError as err:
        return HTTPClientError(
            status_code=529,
            detail=f"Temporary error while rephrasing the query. Please try again later. Error: {err}",
        )
    except InvalidQueryError as exc:
        return HTTPClientError(status_code=412, detail=str(exc))
