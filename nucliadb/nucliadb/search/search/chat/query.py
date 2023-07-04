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
from typing import AsyncIterator, List

from nucliadb_protos.nodereader_pb2 import RelationSearchRequest, RelationSearchResponse
from starlette.responses import StreamingResponse

from nucliadb.search.predict import PredictEngine
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.chat.prompt import format_chat_prompt_content
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    Author,
    ChatModel,
    ChatOptions,
    ChatRequest,
    KnowledgeboxFindResults,
    Message,
    RephraseModel,
)

END_OF_STREAM = "_END_"


async def rephrase_query_from_context(
    kbid: str,
    context: List[Message],
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


async def chat(
    kbid: str,
    find_results: KnowledgeboxFindResults,
    chat_request: ChatRequest,
    user_id: str,
):
    predict = get_predict()
    context = chat_request.context or []
    context.append(
        Message(
            author=Author.NUCLIA,
            text=await format_chat_prompt_content(kbid, find_results),
        )
    )
    chat_model = ChatModel(
        user_id=user_id,
        context=context,
        question=chat_request.query,
        truncate=True,
    )

    ident, generator = await predict.chat_query(kbid, chat_model)

    async def generate_answer(
        results: KnowledgeboxFindResults,
        kbid: str,
        predict: PredictEngine,
        generator: AsyncIterator[bytes],
        features: List[ChatOptions],
    ):
        if ChatOptions.PARAGRAPHS in features:
            bytes_results = base64.b64encode(results.json().encode())
            yield len(bytes_results).to_bytes(length=4, byteorder="big", signed=False)
            yield bytes_results

        answer = []
        async for data in generator:
            answer.append(data)
            yield data

        if ChatOptions.RELATIONS in features:
            yield END_OF_STREAM

            text_answer = b"".join(answer)

            detected_entities = await predict.detect_entities(
                kbid, text_answer.decode()
            )
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

    return StreamingResponse(
        generate_answer(find_results, kbid, predict, generator, chat_request.features),
        media_type="plain/text",
        headers={
            "NUCLIA-LEARNING-ID": ident or "unknown",
            "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
        },
    )
