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
import base64
from typing import Optional, Union

import pydantic
from fastapi import Body, Header, Request, Response
from fastapi.openapi.models import Example
from fastapi_versioning import version
from starlette.responses import StreamingResponse

from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.predict import AnswerStatusCode
from nucliadb.search.search.chat.query import chat, get_relations_results
from nucliadb.search.search.exceptions import IncompleteFindResultsError
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    ChatOptions,
    ChatRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    Relations,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError

END_OF_STREAM = "_END_"


class SyncChatResponse(pydantic.BaseModel):
    answer: str
    relations: Optional[Relations]
    results: KnowledgeboxFindResults
    status: AnswerStatusCode


CHAT_EXAMPLES = {
    "search_and_chat": Example(
        summary="Ask who won the league final",
        description="You can ask a question to your knowledge box",  # noqa
        value={
            "query": "Who won the league final?",
        },
    ),
}


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/chat",
    status_code=200,
    name="Chat Knowledge Box",
    summary="Chat on a Knowledge Box",
    description="Chat on a Knowledge Box",
    tags=["Search"],
    response_model=None,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def chat_knowledgebox_endpoint(
    request: Request,
    kbid: str,
    item: ChatRequest = Body(openapi_examples=CHAT_EXAMPLES),
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
        return await create_chat_response(
            kbid, item, x_nucliadb_user, x_ndb_client, x_forwarded_for, x_synchronous
        )
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
    except predict.SendToPredictError:
        return HTTPClientError(status_code=503, detail="Chat service unavailable")
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


async def create_chat_response(
    kbid: str,
    chat_request: ChatRequest,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    x_synchronous: bool,
) -> Response:
    chat_result = await chat(
        kbid,
        chat_request,
        user_id,
        client_type,
        origin,
    )
    if x_synchronous:
        text_answer = b""
        async for chunk in chat_result.answer_stream:
            text_answer += chunk

        relations_results = None
        if ChatOptions.RELATIONS in chat_request.features:
            relations_results = await get_relations_results(
                kbid=kbid, chat_request=chat_request, text_answer=text_answer
            )

        return Response(
            content=SyncChatResponse(
                answer=text_answer.decode(),
                relations=relations_results,
                results=chat_result.find_results,
                status=chat_result.status_code.value,
            ).json(),
            headers={
                "NUCLIA-LEARNING-ID": chat_result.nuclia_learning_id or "unknown",
                "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
                "Content-Type": "application/json",
            },
        )
    else:

        async def _streaming_response():
            bytes_results = base64.b64encode(chat_result.find_results.json().encode())
            yield len(bytes_results).to_bytes(length=4, byteorder="big", signed=False)
            yield bytes_results

            text_answer = b""
            async for chunk in chat_result.answer_stream:
                text_answer += chunk
                yield chunk

            yield END_OF_STREAM.encode()
            if ChatOptions.RELATIONS in chat_request.features:
                relations_results = await get_relations_results(
                    kbid=kbid, chat_request=chat_request, text_answer=text_answer
                )
                yield base64.b64encode(relations_results.json().encode())

        return StreamingResponse(
            _streaming_response(),
            media_type="application/octet-stream",
            headers={
                "NUCLIA-LEARNING-ID": chat_result.nuclia_learning_id or "unknown",
                "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
            },
        )
