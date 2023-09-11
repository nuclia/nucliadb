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
import asyncio
import base64
from time import monotonic as time
from typing import AsyncIterator, List, Optional

from nucliadb_protos.nodereader_pb2 import RelationSearchRequest, RelationSearchResponse
from starlette.responses import StreamingResponse

from nucliadb.search import logger
from nucliadb.search.predict import AnswerStatusCode, PredictEngine
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.chat.prompt import get_chat_prompt_context
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    Author,
    ChatContextMessage,
    ChatModel,
    ChatOptions,
    ChatRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    RephraseModel,
)
from nucliadb_protos import audit_pb2
from nucliadb_utils.utilities import get_audit

END_OF_STREAM = "_END_"
NOT_ENOUGH_CONTEXT_ANSWER = "Not enough data to answer this."
AUDIT_TEXT_RESULT_SEP = " \n\n "


async def rephrase_query_from_chat_history(
    kbid: str,
    chat_history: List[ChatContextMessage],
    query: str,
    user_id: str,
) -> str:
    predict = get_predict()
    req = RephraseModel(
        question=query,
        chat_history=chat_history,
        user_id=user_id,
    )
    return await predict.rephrase_query(kbid, req)


async def generate_answer(
    *,
    user_query: str,
    query_context: List[str],
    chat_history: List[ChatContextMessage],
    rephrased_query: Optional[str],
    results: KnowledgeboxFindResults,
    kbid: str,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    predict: PredictEngine,
    answer_generator: AsyncIterator[bytes],
    chat_request: ChatRequest,
):
    bytes_results = base64.b64encode(results.json().encode())
    yield len(bytes_results).to_bytes(length=4, byteorder="big", signed=False)
    yield bytes_results

    start_time = time()
    answer = []
    status_code: Optional[AnswerStatusCode] = None
    is_last_chunk = False
    async for answer_chunk, is_last_chunk in async_gen_lookahead(answer_generator):
        if is_last_chunk:
            try:
                status_code = _parse_answer_status_code(answer_chunk)
            except ValueError:
                # TODO: remove this in the future, it's
                # just for bw compatibility until predict
                # is updated to the new protocol
                status_code = AnswerStatusCode.SUCCESS
                answer.append(answer_chunk)
                yield answer_chunk
            else:
                # TODO: this should be needed but, in case we receive the status
                # code mixed with text, we strip it and return the text
                if len(answer_chunk) != len(status_code.encode()):
                    answer_chunk = answer_chunk.rstrip(status_code.encode())
                    yield answer_chunk
            break
        answer.append(answer_chunk)
        yield answer_chunk
    if not is_last_chunk:
        logger.warning("BUG: /chat endpoint without last chunk")

    text_answer = b"".join(answer)

    await maybe_audit_chat(
        kbid=kbid,
        user_id=user_id,
        client_type=client_type,
        origin=origin,
        duration=time() - start_time,
        user_query=user_query,
        rephrased_query=rephrased_query,
        text_answer=text_answer,
        status_code=status_code,
        chat_history=chat_history,
        query_context=query_context,
    )

    yield END_OF_STREAM

    if ChatOptions.RELATIONS in chat_request.features:
        detected_entities = await predict.detect_entities(kbid, text_answer.decode())
        relation_request = RelationSearchRequest()
        relation_request.subgraph.entry_points.extend(detected_entities)
        relation_request.subgraph.depth = 1

        relations_results: List[RelationSearchResponse]
        (
            relations_results,
            _,
            _,
            _,
        ) = await node_query(
            kbid, Method.RELATIONS, relation_request, chat_request.shards
        )
        yield base64.b64encode(
            (
                await merge_relations_results(
                    relations_results, relation_request.subgraph
                )
            )
            .json()
            .encode()
        )


async def not_enough_context_generator():
    await asyncio.sleep(0)
    yield NOT_ENOUGH_CONTEXT_ANSWER.encode()
    yield AnswerStatusCode.NO_CONTEXT.encode()


async def chat(
    kbid: str,
    user_query: str,
    rephrased_query: Optional[str],
    find_results: KnowledgeboxFindResults,
    chat_request: ChatRequest,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
):
    nuclia_learning_id: Optional[str] = None
    chat_history = chat_request.context or []
    predict = get_predict()

    if len(find_results.resources) == 0:
        answer_stream = generate_answer(
            user_query=user_query,
            rephrased_query=rephrased_query,
            results=find_results,
            kbid=kbid,
            user_id=user_id,
            client_type=client_type,
            origin=origin,
            predict=predict,
            answer_generator=not_enough_context_generator(),
            chat_request=chat_request,
            query_context=[],
            chat_history=chat_history,
        )

    else:
        query_context = await get_chat_prompt_context(kbid, find_results)
        chat_model = ChatModel(
            user_id=user_id,
            query_context=query_context,
            chat_history=chat_history,
            question=chat_request.query,
            truncate=True,
        )
        nuclia_learning_id, predict_generator = await predict.chat_query(
            kbid, chat_model
        )
        answer_stream = generate_answer(
            user_query=user_query,
            rephrased_query=rephrased_query,
            results=find_results,
            kbid=kbid,
            user_id=user_id,
            client_type=client_type,
            origin=origin,
            predict=predict,
            answer_generator=predict_generator,
            chat_request=chat_request,
            query_context=query_context,
            chat_history=chat_history,
        )

    return StreamingResponse(
        answer_stream,
        media_type="plain/text",
        headers={
            "NUCLIA-LEARNING-ID": nuclia_learning_id or "unknown",
            "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
        },
    )


async def async_gen_lookahead(gen: AsyncIterator[bytes]):
    """Async generator that yields the next chunk and whether it's the last one.
    Empty chunks are ignored.

    """
    buffered_chunk = None
    async for chunk in gen:
        if buffered_chunk is None:
            # Buffer the first chunk
            buffered_chunk = chunk
            continue

        if len(chunk) == 0:
            continue

        # Yield the previous chunk and buffer the current one
        yield buffered_chunk, False
        buffered_chunk = chunk

    # Yield the last chunk if there is one
    if buffered_chunk is not None:
        yield buffered_chunk, True


def _parse_answer_status_code(chunk: bytes) -> AnswerStatusCode:
    """
    Parses the status code from the last chunk of the answer.
    """
    try:
        return AnswerStatusCode(chunk.decode())
    except ValueError:
        # In some cases, even if the status code was yield separately
        # at the server side, the status code is appended to the previous chunk...
        # It may be a bug in the aiohttp.StreamResponse implementation,
        # but we haven't spotted it yet. For now, we just try to parse the status code
        # from the tail of the chunk.
        logger.warning(
            f"Error decoding status code from /chat's last chunk. Chunk: {chunk!r}"
        )
        if chunk == b"":
            raise
        if chunk.endswith(b"0"):
            return AnswerStatusCode.SUCCESS
        return AnswerStatusCode(chunk[-2:].decode())


async def maybe_audit_chat(
    *,
    kbid: str,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    duration: float,
    user_query: str,
    rephrased_query: Optional[str],
    text_answer: bytes,
    status_code: Optional[AnswerStatusCode],
    chat_history: List[ChatContextMessage],
    query_context: List[str],
):
    audit = get_audit()
    if audit is None:
        return

    audit_answer: Optional[str] = text_answer.decode()
    if status_code == AnswerStatusCode.NO_CONTEXT:
        audit_answer = None

    # Append chat history and query context
    audit_context = [
        audit_pb2.ChatContext(author=message.author, text=message.text)
        for message in chat_history
    ]
    audit_context.append(
        audit_pb2.ChatContext(
            author=Author.NUCLIA,
            text=AUDIT_TEXT_RESULT_SEP.join(query_context),
        )
    )
    await audit.chat(
        kbid,
        user_id,
        client_type.to_proto(),
        origin,
        duration,
        question=user_query,
        rephrased_question=rephrased_query,
        context=audit_context,
        answer=audit_answer,
    )
