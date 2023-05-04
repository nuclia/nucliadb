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
from typing import AsyncIterator, List, Union

from fastapi import Body, Header, Request, Response
from fastapi_versioning import version
from nucliadb_protos.nodereader_pb2 import RelationSearchRequest, RelationSearchResponse
from starlette.responses import StreamingResponse

from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.find import find
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.predict import PredictEngine
from nucliadb.search.requesters.utils import Method, node_query
from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.utilities import get_predict
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    Author,
    ChatModel,
    ChatOptions,
    ChatRequest,
    FindRequest,
    KnowledgeboxFindResults,
    Message,
    NucliaDBClientType,
    SearchOptions,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError

END_OF_STREAM = "_END_"

CHAT_EXAMPLES = {
    "search_and_chat": {
        "summary": "Ask who won the league final",
        "description": "You can ask a question to your knowledge box",  # noqa
        "value": {
            "query": "Who won the league final?",
        },
    },
}


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/chat",
    status_code=200,
    name="Chat Knowledge Box",
    description="Chat on a Knowledge Box",
    tags=["Search"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def chat_post_knowledgebox(
    request: Request,
    response: Response,
    kbid: str,
    item: ChatRequest = Body(examples=CHAT_EXAMPLES),
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Union[StreamingResponse, HTTPClientError]:
    try:
        return await chat(
            response, kbid, item, x_ndb_client, x_nucliadb_user, x_forwarded_for
        )
    except LimitsExceededError as exc:
        return HTTPClientError(status_code=exc.status_code, detail=exc.detail)


async def chat(
    response: Response,
    kbid: str,
    item: ChatRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
):
    predict = get_predict()

    if item.context is not None and len(item.context) > 0:
        # There is context lets do a query
        req = ChatModel(
            question=item.query,
            context=item.context,
            user_id=x_nucliadb_user,
            retrieval=False,
        )

        new_query = await predict.rephrase_query(kbid, req)
    else:
        new_query = item.query

    # ONLY PARAGRAPHS I VECTORS
    find_request = FindRequest()
    find_request.features = [
        SearchOptions.PARAGRAPH,
        SearchOptions.VECTOR,
    ]
    find_request.query = new_query
    find_request.fields = item.fields
    find_request.filters = item.filters
    find_request.field_type_filter = item.field_type_filter
    find_request.min_score = item.min_score
    find_request.range_creation_start = item.range_creation_start
    find_request.range_creation_end = item.range_creation_end
    find_request.range_modification_start = item.range_modification_start
    find_request.range_modification_end = item.range_modification_end
    find_request.show = item.show
    find_request.extracted = item.extracted
    find_request.shards = item.shards

    results = await find(
        response, kbid, find_request, x_ndb_client, x_nucliadb_user, x_forwarded_for
    )

    flattened_text = " \n\n ".join(
        [
            paragraph.text
            for result in results.resources.values()
            for field in result.fields.values()
            for paragraph in field.paragraphs.values()
        ]
    )
    if item.context is None:
        context = []
    else:
        context = item.context
    context.append(Message(author=Author.NUCLIA, text=flattened_text))

    chat_model = ChatModel(
        user_id=x_nucliadb_user, context=context, question=item.query
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
                incomplete_results,
                queried_nodes,
                queried_shards,
            ) = await node_query(kbid, Method.RELATIONS, relation_request, item.shards)
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
        generate_answer(results, kbid, predict, generator, item.features),
        media_type="plain/text",
        headers={
            "NUCLIA-LEARNING-ID": ident,
            "Access-Control-Expose-Headers": "NUCLIA-LEARNING-ID",
        },
    )
