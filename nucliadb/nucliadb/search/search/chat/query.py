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

from nucliadb_protos.audit_pb2 import ChatContext
from nucliadb_protos.nodereader_pb2 import RelationSearchRequest, RelationSearchResponse
from starlette.responses import StreamingResponse

from nucliadb.search.predict import AnswerStatusCode, PredictEngine
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.chat.prompt import format_chat_prompt_content
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
from nucliadb_utils.utilities import get_audit

END_OF_STREAM = "_END_"
NOT_ENOUGH_CONTEXT_ANSWER = "Not enough data to answer this."


async def rephrase_query_from_context(
    kbid: str,
    context: List[ChatContextMessage],
    query: str,
    user_id: str,
) -> str:
    predict = get_predict()
    req = RephraseModel(
        question=query,
        context=context,
        user_id=user_id,
    )
    return await predict.rephrase_query(kbid, req)


async def generate_answer(
    user_query: str,
    user_context: List[ChatContextMessage],
    rephrase_query: Optional[str],
    results: KnowledgeboxFindResults,
    kbid: str,
    user_id: str,
    client_type: NucliaDBClientType,
    origin: str,
    predict: Optional[PredictEngine],
    answer_generator: AsyncIterator[bytes],
    chat_request: ChatRequest,
    do_audit: bool = True,
):
    audit = get_audit()
    if ChatOptions.PARAGRAPHS in chat_request.features:
        bytes_results = base64.b64encode(results.json().encode())
        yield len(bytes_results).to_bytes(length=4, byteorder="big", signed=False)
        yield bytes_results

    start_time = time()
    answer = []
    status_code: Optional[AnswerStatusCode] = None
    async for answer_chunk, is_last_chunk in async_gen_lookahead(answer_generator):
        if is_last_chunk:
            try:
                status_code = AnswerStatusCode(answer_chunk.decode())
            except ValueError:
                # TODO: remove this in the future, it's
                # just for bw compatibility until predict
                # is updated to the new protocol
                status_code = AnswerStatusCode.SUCCESS
                answer.append(answer_chunk)
                yield answer_chunk
            break
        answer.append(answer_chunk)
        yield answer_chunk

    text_answer = b"".join(answer)

    if do_audit and audit is not None:
        audit_answer: Optional[str] = text_answer.decode()
        if status_code == AnswerStatusCode.NO_CONTEXT:
            audit_answer = None

        context = [
            ChatContext(author=message.author, text=message.text)
            for message in user_context
        ]
        await audit.chat(
            kbid,
            user_id,
            client_type.to_proto(),
            origin,
            time() - start_time,
            question=user_query,
            rephrased_question=rephrase_query,
            context=context,
            answer=audit_answer,
        )

    if ChatOptions.RELATIONS in chat_request.features and predict is not None:
        yield END_OF_STREAM

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
    user_context = chat_request.context or []

    if len(find_results.resources) == 0:
        answer_stream = generate_answer(
            user_query,
            user_context,
            rephrased_query,
            find_results,
            kbid,
            user_id,
            client_type,
            origin,
            predict=None,
            answer_generator=not_enough_context_generator(),
            chat_request=chat_request,
        )

    else:
        predict = get_predict()
        chat_context = user_context[:]
        chat_context.append(
            ChatContextMessage(
                author=Author.NUCLIA,
                text=await format_chat_prompt_content(kbid, find_results),
            )
        )
        chat_model = ChatModel(
            user_id=user_id,
            context=chat_context,
            question=chat_request.query,
            truncate=True,
        )
        nuclia_learning_id, predict_generator = await predict.chat_query(
            kbid, chat_model
        )
        answer_stream = generate_answer(
            user_query,
            user_context,
            rephrased_query,
            find_results,
            kbid,
            user_id,
            client_type,
            origin,
            predict=predict,
            answer_generator=predict_generator,
            chat_request=chat_request,
        )

    return StreamingResponse(
        answer_stream,
        media_type="plain/text",
        headers={
            "NUCLIA-LEARNING-ID": nuclia_learning_id or "unknown",
            "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
        },
    )


async def async_gen_lookahead(gen):
    """
    Async generator that yields the next chunk and whether it's the last one.
    """
    buffered_chunk = None
    async for chunk in gen:
        if buffered_chunk is None:
            # Buffer the first chunk
            buffered_chunk = chunk
            continue

        # Yield the previous chunk and buffer the current one
        yield buffered_chunk, False
        buffered_chunk = chunk

    # Yield the last chunk if there is one
    if buffered_chunk is not None:
        yield buffered_chunk, True
