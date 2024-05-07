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
from typing import Any, Optional, Union

import pydantic
from fastapi import Body, Header, Request, Response
from fastapi.openapi.models import Example
from fastapi_versioning import version
from starlette.responses import StreamingResponse

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.models.responses import HTTPClientError
from nucliadb.search import predict
from nucliadb.search.api.v1.resource.ask import ASK_EXAMPLES
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.predict import AnswerStatusCode
from nucliadb.search.search.chat.query import (
    ask,
)
from nucliadb.search.search.exceptions import (
    IncompleteFindResultsError,
    InvalidQueryError,
)
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    AskRequest,
    KnowledgeboxFindResults,
    MaxTokens,
    NucliaDBClientType,
    PromptContext,
    PromptContextOrder,
    Relations,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError

END_OF_STREAM = "_END_"


class SyncAskResponse(pydantic.BaseModel):
    answer: str
    relations: Optional[Relations]
    results: KnowledgeboxFindResults
    status: AnswerStatusCode
    citations: dict[str, Any] = {}
    prompt_context: Optional[PromptContext] = None
    prompt_context_order: Optional[PromptContextOrder] = None


ASK_EXAMPLES = {
    "ask": Example(
        summary="Ask who won the league final",
        description="You can ask a question to your knowledge box",  # noqa
        value={
            "query": "Who won the league final?",
        },
    ),
    "ask_with_custom_prompt": Example(
        summary="Ask for the gold price evolution in 2023 in a very conscise way",
        description="You can ask a question and specify a custom prompt to tweak the tone of the response",  # noqa
        value={
            "query": "How has the price of gold evolved during 2023?",
            "prompt": "Given this context: {context}. Answer this {question} in a concise way using the provided context",  # noqa
        },
    ),
}


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/ask",
    status_code=200,
    name="Ask Knowledge Box",
    summary="Ask questions on a Knowledge Box",
    description="Ask questions on a Knowledge Box",
    tags=["Search"],
    response_model=None,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def ask_knowledgebox_endpoint(
    request: Request,
    kbid: str,
    item: AskRequest = Body(openapi_examples=ASK_EXAMPLES),
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
        return await create_ask_response(
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
    ask_result = await ask(
        kbid,
        ask_request,
        user_id,
        client_type,
        origin,
        resource=resource,
    )
    if x_synchronous:
        # Run the stream in memory to get all the data in memory
        async for _ in ask_result.stream():
            ...

        # Return it as a JSON response
        sync_ask_response = SyncAskResponse(
            answer=ask_result.answer,
            relations=ask_result.relations_results,
            results=ask_result.find_results,
            status=ask_result.status_code.value,
            citations=ask_result.citations,
        )

        if ask_request.debug:
            sync_ask_response.prompt_context = ask_result.prompt_context
            sync_ask_response.prompt_context_order = ask_result.prompt_context_order

        return Response(
            content=sync_ask_response.json(exclude_unset=True),
            headers={
                "NUCLIA-LEARNING-ID": ask_result.nuclia_learning_id or "unknown",
                "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
                "Content-Type": "application/json",
            },
        )
    else:
        return StreamingResponse(
            ask_result.stream(),
            media_type="application/octet-stream",
            headers={
                "NUCLIA-LEARNING-ID": ask_result.nuclia_learning_id or "unknown",
                "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
            },
        )


def parse_max_tokens(
    max_tokens: Optional[Union[int, MaxTokens]]
) -> Optional[MaxTokens]:
    if isinstance(max_tokens, int):
        # If the max_tokens is an integer, it is interpreted as the max_tokens value for the generated answer.
        # The max tokens for the context is set to None to use the default value for the model (comes in the
        # NUA's query endpoint response).
        return MaxTokens(answer=max_tokens, context=None)
    return max_tokens
