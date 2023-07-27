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
from time import monotonic as time
from typing import AsyncIterator, List, Optional

from nucliadb_protos.audit_pb2 import ChatContext
from nucliadb_protos.nodereader_pb2 import RelationSearchRequest, RelationSearchResponse
from starlette.responses import StreamingResponse

from nucliadb.search.predict import PredictEngine
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
    predict: PredictEngine,
    predict_generator: AsyncIterator[bytes],
    chat_request: ChatRequest,
    do_audit: bool = True,
):
    audit = get_audit()
    if ChatOptions.PARAGRAPHS in chat_request.features:
        bytes_results = base64.b64encode(results.json().encode())
        yield len(bytes_results).to_bytes(length=4, byteorder="big", signed=False)
        yield bytes_results

    if results.total == 0:
        yield NOT_ENOUGH_CONTEXT_ANSWER.encode()
        return

    start_time = time()
    answer = []
    async for data in predict_generator:
        answer.append(data)
        yield data

    text_answer = b"".join(answer)

    if do_audit and audit is not None:
        decoded_answer = text_answer.decode()
        audit_answer = (
            decoded_answer if decoded_answer != NOT_ENOUGH_CONTEXT_ANSWER else None
        )

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

    if ChatOptions.RELATIONS in chat_request.features:
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
    predict = get_predict()

    user_context = chat_request.context or []
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

    ident, generator = await predict.chat_query(kbid, chat_model)

    return StreamingResponse(
        generate_answer(
            user_query,
            user_context,
            rephrased_query,
            find_results,
            kbid,
            user_id,
            client_type,
            origin,
            predict,
            generator,
            chat_request=chat_request,
        ),
        media_type="plain/text",
        headers={
            "NUCLIA-LEARNING-ID": ident or "unknown",
            "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
        },
    )
