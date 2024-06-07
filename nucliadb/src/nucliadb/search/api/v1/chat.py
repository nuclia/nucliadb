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
import json
from typing import Any, Optional, Union

import pydantic
from fastapi import Body, Header, Request, Response
from fastapi.openapi.models import Example
from fastapi_versioning import version
from starlette.responses import StreamingResponse

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import logger, predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.predict import AnswerStatusCode
from nucliadb.search.search.chat.query import (
    START_OF_CITATIONS,
    chat,
    get_relations_results,
)
from nucliadb.search.search.exceptions import (
    IncompleteFindResultsError,
    InvalidQueryError,
)
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    ChatOptions,
    ChatRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    PromptContext,
    PromptContextOrder,
    Relations,
    parse_max_tokens,
)
from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError

END_OF_STREAM = "_END_"


class SyncChatResponse(pydantic.BaseModel):
    answer: str
    relations: Optional[Relations] = None
    results: KnowledgeboxFindResults
    status: AnswerStatusCode
    citations: dict[str, Any] = {}
    prompt_context: Optional[PromptContext] = None
    prompt_context_order: Optional[PromptContextOrder] = None


CHAT_EXAMPLES = {
    "search_and_chat": Example(
        summary="Ask who won the league final",
        description="You can ask a question to your knowledge box",  # noqa
        value={
            "query": "Who won the league final?",
        },
    ),
    "search_and_chat_with_custom_prompt": Example(
        summary="Ask for the gold price evolution in 2023 in a very conscise way",
        description="You can ask a question and specify a custom prompt to tweak the tone of the response",  # noqa
        value={
            "query": "How has the price of gold evolved during 2023?",
            "prompt": "Given this context: {context}. Answer this {question} in a concise way using the provided context",  # noqa
        },
    ),
}


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/chat",
    status_code=200,
    summary="Chat on a Knowledge Box",
    description="Chat on a Knowledge Box",
    tags=["Search"],
    response_model=None,
    deprecated=True,
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
        description="When set to true, outputs response as JSON in a non-streaming way. "
        "This is slower and requires waiting for entire answer to be ready.",
    ),
) -> Union[StreamingResponse, HTTPClientError, Response]:
    try:
        return await create_chat_response(
            kbid, item, x_nucliadb_user, x_ndb_client, x_forwarded_for, x_synchronous
        )
    except KnowledgeBoxNotFound:
        return HTTPClientError(
            status_code=404,
            detail=f"Knowledge Box '{kbid}' not found.",
        )
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)
    except predict.ProxiedPredictAPIError as err:
        return HTTPClientError(
            status_code=err.status,
            detail=err.detail,
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


async def create_chat_response(
    kbid: str,
    chat_request: ChatRequest,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    x_synchronous: bool,
    resource: Optional[str] = None,
) -> Response:
    chat_request.max_tokens = parse_max_tokens(chat_request.max_tokens)
    chat_result = await chat(
        kbid,
        chat_request,
        user_id,
        client_type,
        origin,
        resource=resource,
    )
    if x_synchronous:
        streamed_answer = b""
        async for chunk in chat_result.answer_stream:
            streamed_answer += chunk

        answer, citations = parse_streamed_answer(streamed_answer, chat_request.citations)

        relations_results = None
        if ChatOptions.RELATIONS in chat_request.features:
            # XXX should use query parser here
            relations_results = await get_relations_results(
                kbid=kbid, text_answer=answer, target_shard_replicas=chat_request.shards
            )

        sync_chat_resp = SyncChatResponse(
            answer=answer,
            relations=relations_results,
            results=chat_result.find_results,
            status=chat_result.status_code.value,
            citations=citations,
        )
        if chat_request.debug:
            sync_chat_resp.prompt_context = chat_result.prompt_context
            sync_chat_resp.prompt_context_order = chat_result.prompt_context_order
        return Response(
            content=sync_chat_resp.json(exclude_unset=True),
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

            streamed_answer = b""
            async for chunk in chat_result.answer_stream:
                streamed_answer += chunk
                yield chunk

            answer, _ = parse_streamed_answer(streamed_answer, chat_request.citations)

            yield END_OF_STREAM.encode()
            if ChatOptions.RELATIONS in chat_request.features:
                # XXX should use query parser here
                relations_results = await get_relations_results(
                    kbid=kbid,
                    text_answer=answer,
                    target_shard_replicas=chat_request.shards,
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


def parse_streamed_answer(
    streamed_bytes: bytes, requested_citations: bool
) -> tuple[str, dict[str, Any]]:
    try:
        text_answer, tail = streamed_bytes.split(START_OF_CITATIONS, 1)
    except ValueError:
        if requested_citations:
            logger.warning(
                "Citations were requested but not found in the answer. "
                "Returning the answer without citations."
            )
        return streamed_bytes.decode("utf-8"), {}
    if not requested_citations:
        logger.warning(
            "Citations were not requested but found in the answer. "
            "Returning the answer without citations."
        )
        return text_answer.decode("utf-8"), {}
    try:
        citations_length = int.from_bytes(tail[:4], byteorder="big", signed=False)
        citations_bytes = tail[4 : 4 + citations_length]
        citations = json.loads(base64.b64decode(citations_bytes).decode())
        return text_answer.decode("utf-8"), citations
    except Exception as exc:
        capture_exception(exc)
        logger.exception("Error parsing citations. Returning the answer without citations.")
        return text_answer.decode("utf-8"), {}
