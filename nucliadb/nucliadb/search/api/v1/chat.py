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
from typing import Any, AsyncGenerator, Optional, Union

import pydantic
from fastapi import Body, Header, Request, Response
from fastapi.openapi.models import Example
from fastapi_versioning import version
from starlette.responses import StreamingResponse

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.predict import AnswerStatusCode
from nucliadb.search.search.ask import AskResult, ask
from nucliadb.search.search.chat.query import START_OF_CITATIONS
from nucliadb.search.search.exceptions import (
    IncompleteFindResultsError,
    InvalidQueryError,
)
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    AnswerAskResponseItem,
    AskRequest,
    AskResponseItem,
    ChatRequest,
    CitationsAskResponseItem,
    DebugAskResponseItem,
    KnowledgeboxFindResults,
    MetadataAskResponseItem,
    NucliaDBClientType,
    PromptContext,
    PromptContextOrder,
    Relations,
    RelationsAskResponseItem,
    RetrievalAskResponseItem,
    StatusAskResponseItem,
    parse_max_tokens,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError

END_OF_STREAM = b"_END_"


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
    """
    Chat endpoint is deprecated.
    Internally, we use the ask logic but convert the responses to keep the same
    behavior as the chat endpoint until it is removed.
    """
    chat_request.max_tokens = parse_max_tokens(chat_request.max_tokens)
    ask_request = AskRequest.parse_obj(chat_request.dict())
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
        sync_ask_response = await ask_result.sync_response()
        sync_chat_response = to_chat_sync_response(sync_ask_response)
        return Response(
            content=sync_chat_response.json(exclude_unset=True),
            status_code=200,
            headers=headers,
            media_type="application/json",
        )
    else:
        return StreamingResponse(
            content=to_chat_stream_response(ask_result),
            status_code=200,
            headers=headers,
            media_type="application/octet-stream",
        )


"""
    if x_synchronous:


        streamed_answer = b""
        async for chunk in chat_result.answer_stream:
            streamed_answer += chunk

        answer, citations = parse_streamed_answer(
            streamed_answer, chat_request.citations
        )

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
"""


def to_chat_sync_response(ask_result: AskResult) -> SyncChatResponse:
    citations = {}
    if ask_result._citations is not None:
        citations = ask_result._citations.citations
    return SyncChatResponse(
        answer=ask_result.answer,
        relations=ask_result._relations,
        results=ask_result.find_results,
        status=ask_result.status_code,
        citations=citations,
        prompt_context=ask_result.prompt_context,
        prompt_context_order=ask_result.prompt_context_order,
    )


async def to_chat_stream_response(ask_result: AskResult) -> AsyncGenerator[bytes, None]:
    # First off, stream the find results
    find_results = ask_result.find_results
    bytes_results = base64.b64encode(find_results.json().encode())
    yield len(bytes_results).to_bytes(length=4, byteorder="big", signed=False)
    yield bytes_results

    end_of_string_yielded = False

    # Then stream the answer, the status code and the citations
    async for nd_json_line in ask_result.ndjson_stream():
        ask_response_item = AskResponseItem.model_validate_json(nd_json_line).item
        if isinstance(
            ask_response_item,
            (
                RetrievalAskResponseItem,  # already streamed
                MetadataAskResponseItem,  # not supported in chat
                DebugAskResponseItem,  # not supported in chat
            ),
        ):
            continue

        if isinstance(ask_response_item, AnswerAskResponseItem):
            yield ask_response_item.text.encode()

        elif isinstance(ask_response_item, StatusAskResponseItem):
            yield ask_response_item.code.encode()

        elif isinstance(ask_response_item, CitationsAskResponseItem):
            yield START_OF_CITATIONS
            citations = ask_response_item.citations
            encoded_citations = base64.b64encode(json.dumps(citations).encode())
            yield len(encoded_citations).to_bytes(
                length=4, byteorder="big", signed=False
            )
            yield encoded_citations

        elif isinstance(ask_response_item, RelationsAskResponseItem):
            yield END_OF_STREAM
            end_of_string_yielded = True

            relations = ask_response_item.relations
            yield base64.b64encode(relations.json().encode())

    if not end_of_string_yielded:
        yield END_OF_STREAM
