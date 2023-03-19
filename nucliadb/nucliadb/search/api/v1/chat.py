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

from fastapi import Body, Header, Request, Response
from fastapi_versioning import version
from nucliadb_protos.nodereader_pb2 import RelationSearchRequest, RelationSearchResponse
from starlette.responses import StreamingResponse

from nucliadb.search.api.v1.find import find
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.predict import PredictEngine
from nucliadb.search.requesters.utils import Method, query
from nucliadb.search.utilities import get_predict
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.search import (
    ChatModel,
    ChatRequest,
    FindRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
    SearchOptions,
)
from nucliadb_utils.authentication import requires

END_OF_STREAM = "_END_"

CHAT_EXAMPLES = {
    "search_and_chat": {
        "summary": "Search for pdf documents where the text 'Noam Chomsky' appears",
        "description": "For a complete list of filters, visit: https://github.com/nuclia/nucliadb/blob/main/docs/internal/SEARCH.md#filters-and-facets",  # noqa
        "value": {
            "query": "Noam Chomsky",
            "filters": ["/n/i/application/pdf"],
            "features": [SearchOptions.DOCUMENT],
        },
    },
    "get_language_counts": {
        "summary": "Get the number of documents for each language",
        "description": "For a complete list of facets, visit: https://github.com/nuclia/nucliadb/blob/main/docs/internal/SEARCH.md#filters-and-facets",  # noqa
        "value": {
            "page_size": 0,
            "faceted": ["/s/p"],
            "features": [SearchOptions.DOCUMENT],
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
) -> StreamingResponse:
    predict = get_predict()

    if len(item.context) > 0:
        # There is context lets do a query
        req = ChatModel()
        req.context = item.context
        # req.system = "You help creating new queries"
        req.question = "Which question should be done to answer: " + item.query

        new_query_elements = []
        async for new_query_data in predict.chat_query(kbid, req):
            new_query_elements.append(new_query_data)

        new_query = "".join(new_query_elements)
    else:
        new_query = item.query

    # ONLY PARAGRAPHS I VECTORS
    find_request = FindRequest()
    find_request.features = [
        SearchOptions.PARAGRAPH,
        SearchOptions.VECTOR,
    ]
    find_request.query = new_query
    find_request.filters = item.filters
    find_request.field_type_filter = item.field_type_filter
    find_request.fields = item.fields

    results = await find(
        response, kbid, find_request, x_ndb_client, x_nucliadb_user, x_forwarded_for
    )

    async def generate_answer(
        results: KnowledgeboxFindResults, kbid: str, predict: PredictEngine
    ):
        results = base64.b64encode(results.json())
        yield len(results).to_bytes(length=4, byteorder="big", signed=False)
        yield results

        answer = []
        async for data in predict.chat_query(kbid, item.context):
            answer.append(data)
            yield data
        yield END_OF_STREAM

        detected_entities = await predict.detect_entities(kbid, "".join(answer))
        relation_request = RelationSearchRequest()
        relation_request.subgraph.entry_points.extend(detected_entities)
        relation_request.subgraph.depth = 1

        relations_results: RelationSearchResponse
        (
            relations_results,
            incomplete_results,
            queried_nodes,
            queried_shards,
        ) = await query(kbid, Method.RELATIONS, relation_request, item.shards)
        yield base64.b64encode(relations_results)

    return StreamingResponse(
        generate_answer(results, kbid, predict),
        media_type="plain/text",
    )
