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
import math

from google.protobuf.json_format import ParseDict
from nidx_protos.nodereader_pb2 import (
    DocumentScored,
    DocumentVectorIdentifier,
    GraphSearchRequest,
    GraphSearchResponse,
    ParagraphResult,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
)

from nucliadb.common.ids import ParagraphId
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.marklogic import documents


async def hybrid_search(kbid: str, query: SearchRequest) -> SearchResponse:
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver):
        return SearchResponse()

    response = SearchResponse()
    if query.paragraph and query.body:
        await _search_chunks(driver, kbid, query, response)
    if query.vector:
        await _search_vectors(driver, kbid, query, response)
    return response


async def _search_chunks(
    driver: MarkLogicDriver, kbid: str, query: SearchRequest, response: SearchResponse
) -> None:
    javascript = (
        "cts.search(cts.andQuery(["
        f"cts.collectionQuery({json.dumps(MarkLogicCollections.CHUNKS)}), "
        f"cts.wordQuery({json.dumps(query.body.split())})"
        "]), ['score-bm25']).toArray().map(doc => "
        "({uri: xdmp.nodeUri(doc), score: cts.score(doc)}))"
    )
    async with driver.ro_transaction(kbid=kbid) as txn:
        response.paragraph.query = query.body
        matches = await documents.evaluate(txn, javascript)
        if len(matches) == 1 and isinstance(matches[0], list):
            matches = matches[0]
        for match in matches:
            if not isinstance(match, dict):
                continue
            uri = match.get("uri")
            if not isinstance(uri, str):
                continue
            content = await documents.read(txn, uri)
            if content is None:
                continue
            paragraph_id = ParagraphId.from_string(str(content["chunk_id"]))
            result = ParagraphResult(
                uuid=paragraph_id.rid,
                field=paragraph_id.field_id.short_without_subfield(),
                start=paragraph_id.paragraph_start,
                end=paragraph_id.paragraph_end,
                paragraph=content["chunk_id"],
                index=int(content.get("data", {}).get("position", {}).get("index", 0)),
                labels=content.get("facets", []),
            )
            result.score.bm25 = float(match.get("score", 0.0))
            ParseDict(content.get("data", {}), result.metadata)
            response.paragraph.results.append(result)
        response.paragraph.total = len(response.paragraph.results)


async def _search_vectors(
    driver: MarkLogicDriver, kbid: str, query: SearchRequest, response: SearchResponse
) -> None:
    javascript = (
        "cts.uris('', ['document', 'item-order'], cts.collectionQuery("
        f"{json.dumps(MarkLogicCollections.EMBEDDINGS)}))"
    )
    async with driver.ro_transaction(kbid=kbid) as txn:
        uris = await documents.evaluate(txn, javascript)
        scored: list[tuple[float, dict]] = []
        query_norm = math.sqrt(sum(value * value for value in query.vector))
        if query_norm == 0:
            return
        for uri in uris:
            embedding = await documents.read(txn, str(uri))
            if embedding is None or embedding.get("model_id") != query.vectorset:
                continue
            vector = embedding.get("vector", [])
            if len(vector) != len(query.vector):
                continue
            vector_norm = math.sqrt(sum(value * value for value in vector))
            if vector_norm == 0:
                continue
            similarity = sum(left * right for left, right in zip(query.vector, vector)) / (
                query_norm * vector_norm
            )
            if similarity < query.min_score_semantic:
                continue
            scored.append((similarity, embedding))

        scored.sort(key=lambda item: item[0], reverse=True)
        for score, embedding in scored[: query.result_per_page or None]:
            result = DocumentScored(
                doc_id=DocumentVectorIdentifier(id=embedding["chunk_id"]),
                score=score,
                labels=embedding.get("facets", []),
            )
            ParseDict(embedding.get("data", {}), result.metadata)
            response.vector.documents.append(result)


async def suggest(kbid: str, query: SuggestRequest) -> SuggestResponse:
    # TODO(Marklogic): Execute the suggestion request against the indexing backend.
    return SuggestResponse()


async def graph_search(kbid: str, query: GraphSearchRequest) -> GraphSearchResponse:
    # TODO(Marklogic): Execute the graph request against the indexing backend.
    return GraphSearchResponse()
