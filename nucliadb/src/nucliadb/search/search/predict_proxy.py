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
import json
from enum import Enum
from typing import Any, Optional, Union

import aiohttp
from fastapi.datastructures import QueryParams
from fastapi.responses import Response, StreamingResponse
from multidict import CIMultiDictProxy
from nuclia_models.predict.generative_responses import (
    GenerativeChunk,
    JSONGenerativeResponse,
    StatusGenerativeResponse,
    TextGenerativeResponse,
)
from pydantic import ValidationError

from nucliadb.common import datamanagers
from nucliadb.search import logger
from nucliadb.search.predict import (
    NUCLIA_LEARNING_ID_HEADER,
    NUCLIA_LEARNING_MODEL_HEADER,
    AnswerStatusCode,
    PredictEngine,
)
from nucliadb.search.search.chat.query import maybe_audit_chat
from nucliadb.search.search.metrics import AskMetrics
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import NucliaDBClientType


class PredictProxiedEndpoints(str, Enum):
    """
    Enum for the different endpoints that are proxied to the Predict API
    """

    TOKENS = "tokens"
    CHAT = "chat"
    REPHRASE = "rephrase"
    RUN_AGENTS_TEXT = "run-agents-text"
    SUMMARIZE = "summarize"
    RERANK = "rerank"
    REMI = "remi"


ALLOWED_HEADERS = [
    "Accept",  # To allow 'application/x-ndjson' on the /chat endpoint
    "X-show-consumption",  # To show token consumption in the response
]

PREDICT_ANSWER_METRIC = "predict_answer_proxy_metric"


async def predict_proxy(
    kbid: str,
    endpoint: PredictProxiedEndpoints,
    method: str,
    params: QueryParams,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    json: Optional[Any] = None,
    headers: dict[str, str] = {},
) -> Union[Response, StreamingResponse]:
    if not await exists_kb(kbid=kbid):
        raise datamanagers.exceptions.KnowledgeBoxNotFound()

    predict: PredictEngine = get_predict()
    predict_headers = predict.get_predict_headers(kbid)
    user_headers = {k: v for k, v in headers.items() if k.capitalize() in ALLOWED_HEADERS}

    # Proxy the request to predict API
    predict_response = await predict.make_request(
        method=method,
        url=predict.get_predict_url(endpoint, kbid),
        json=json,
        params=params,
        headers={**user_headers, **predict_headers},
    )

    status_code = predict_response.status
    media_type = predict_response.headers.get("Content-Type")
    response: Union[Response, StreamingResponse]
    user_query = json.get("question") if json is not None else ""
    if predict_response.headers.get("Transfer-Encoding") == "chunked":
        if endpoint == PredictProxiedEndpoints.CHAT:
            streaming_generator = chat_streaming_generator(
                predict_response=predict_response,
                kbid=kbid,
                user_id=user_id,
                client_type=client_type,
                origin=origin,
                user_query=user_query,
                is_json="json" in (media_type or ""),
            )
        else:
            streaming_generator = predict_response.content.iter_any()

        response = StreamingResponse(
            content=streaming_generator,
            status_code=status_code,
            media_type=media_type,
        )
    else:
        metrics = AskMetrics()
        with metrics.time(PREDICT_ANSWER_METRIC):
            content = await predict_response.read()

        if endpoint == PredictProxiedEndpoints.CHAT:
            try:
                llm_status_code = int(content[-1:].decode())  # Decode just the last char
                if llm_status_code != 0:
                    llm_status_code = -llm_status_code
            except ValueError:
                llm_status_code = -1

            audit_predict_proxy_endpoint(
                predict_response.headers,
                kbid=kbid,
                user_id=user_id,
                user_query=user_query,
                client_type=client_type,
                origin=origin,
                text_answer=content,
                generative_answer_time=metrics[PREDICT_ANSWER_METRIC],
                generative_answer_first_chunk_time=None,
                status_code=AnswerStatusCode(str(llm_status_code)),
            )

        response = Response(
            content=content,
            status_code=status_code,
            media_type=media_type,
        )

    nuclia_learning_id = predict_response.headers.get("NUCLIA-LEARNING-ID")
    if nuclia_learning_id:
        response.headers["NUCLIA-LEARNING-ID"] = nuclia_learning_id
        response.headers["Access-Control-Expose-Headers"] = "NUCLIA-LEARNING-ID"
    return response


async def exists_kb(kbid: str) -> bool:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.kb.exists_kb(txn, kbid=kbid)


async def chat_streaming_generator(
    predict_response: aiohttp.ClientResponse,
    kbid: str,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    user_query: str,
    is_json: bool,
):
    first = True
    status_code = AnswerStatusCode.ERROR.value
    text_answer = ""
    json_object = None
    metrics = AskMetrics()
    with metrics.time(PREDICT_ANSWER_METRIC):
        async for chunk in predict_response.content:
            if first:
                metrics.record_first_chunk_yielded()
                first = False

            yield chunk

            if is_json:
                try:
                    parsed_chunk = GenerativeChunk.model_validate_json(chunk).chunk
                    if isinstance(parsed_chunk, TextGenerativeResponse):
                        text_answer += parsed_chunk.text
                    elif isinstance(parsed_chunk, JSONGenerativeResponse):
                        json_object = parsed_chunk.object
                    elif isinstance(parsed_chunk, StatusGenerativeResponse):
                        status_code = parsed_chunk.code
                except ValidationError:
                    logger.exception(
                        f"Unexpected item in predict answer stream: {chunk.decode()}",
                        extra={"kbid": kbid},
                    )
            else:
                text_answer += chunk.decode()

    if is_json is False and chunk:  # Ensure chunk is not empty before decoding
        # If response is text the status_code comes at the last chunk of data
        last_chunk = chunk.decode()
        if last_chunk[-1] == "0":
            status_code = "0"
        else:
            status_code = last_chunk[-2:]

    audit_predict_proxy_endpoint(
        headers=predict_response.headers,
        kbid=kbid,
        user_id=user_id,
        user_query=user_query,
        client_type=client_type,
        origin=origin,
        text_answer=text_answer.encode() if json_object is None else json.dumps(json_object).encode(),
        generative_answer_time=metrics[PREDICT_ANSWER_METRIC],
        generative_answer_first_chunk_time=metrics.get_first_chunk_time(),
        status_code=AnswerStatusCode(status_code),
    )


def audit_predict_proxy_endpoint(
    headers: CIMultiDictProxy,
    kbid: str,
    user_id: str,
    user_query: str,
    client_type: NucliaDBClientType,
    origin: str,
    text_answer: bytes,
    generative_answer_time: float,
    generative_answer_first_chunk_time: Optional[float],
    status_code: AnswerStatusCode,
):
    maybe_audit_chat(
        kbid=kbid,
        user_id=user_id,
        client_type=client_type,
        origin=origin,
        user_query=user_query,
        rephrased_query=None,
        retrieval_rephrase_query=None,
        chat_history=[],
        learning_id=headers.get(NUCLIA_LEARNING_ID_HEADER),
        query_context={},
        query_context_order={},
        model=headers.get(NUCLIA_LEARNING_MODEL_HEADER),
        text_answer=text_answer,
        generative_answer_time=generative_answer_time,
        generative_answer_first_chunk_time=generative_answer_first_chunk_time or 0,
        rephrase_time=None,
        status_code=status_code,
    )
