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
from unittest.mock import AsyncMock, Mock, patch

import pytest
from httpx import AsyncClient
from pydantic import ValidationError

from nucliadb.common.ids import ParagraphId
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb.search.search.query_parser.models import ParsedQuery
from nucliadb.search.search.query_parser.parsers.retrieve import parse_retrieve
from nucliadb.tests.vectors import Q
from nucliadb_models.retrieval import (
    KeywordOverrides,
    KeywordQuery,
    Query,
    QueryOverrides,
    RawQuery,
    Rephrase,
    RetrievalRequest,
    RetrievalResponse,
    SemanticQuery,
)
from nucliadb_models.search import Image, PredictReranker, RerankerName
from nucliadb_protos import knowledgebox_pb2
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import clothing_store_resources, smb_wonder_resource


@pytest.mark.deploy_modes("standalone")
async def test_retrieve(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    """Basic test for the /retrieve endpoint with a keyword and semantic query.

    We expect to match all paragraphs from the smb wonder resource. One of them
    should only match semantic, and we'll use it to check proper score history.

    """

    kbid = knowledgebox
    rid = await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/retrieve",
        json={
            "query": {
                "keyword": {
                    "query": "smb wonder",
                    "min_score": 0.0,
                },
                "semantic": {
                    "query": Q,
                    "vectorset": "my-semantic-model",
                    "min_score": -1,
                },
            },
            "top_k": 10,
        },
    )
    assert resp.status_code == 200

    body = RetrievalResponse.model_validate(resp.json())

    # we expect to match all paragraphs in the resource
    paragraph_ids = [match.id for match in body.matches]
    expected_paragraph_ids = [
        f"{rid}/a/title/0-24",
        f"{rid}/a/summary/0-44",
        f"{rid}/f/smb-wonder/0-99",
        f"{rid}/f/smb-wonder/99-145",
        f"{rid}/f/smb-wonder/145-234",
    ]
    assert sorted(paragraph_ids) == sorted(expected_paragraph_ids)

    # the best match has been found in both indexes and RRF boosted it
    assert len(body.matches[0].score.history) == 3
    assert body.matches[0].score.history[0].source == "index"
    assert body.matches[0].score.history[0].type == "keyword"
    assert body.matches[0].score.history[1].source == "index"
    assert body.matches[0].score.history[1].type == "semantic"
    assert body.matches[0].score.history[2].source == "rank_fusion"
    assert body.matches[0].score.history[2].type == "rrf"

    # this paragraph has no keyord match, so we matched it due to semantic
    matched_only_with_semantic = next(
        filter(
            lambda match: match.id == f"{rid}/f/smb-wonder/145-234",
            body.matches,
        )
    )
    assert len(matched_only_with_semantic.score.history) == 2
    assert matched_only_with_semantic.score.history[0].source == "index"
    assert matched_only_with_semantic.score.history[0].type == "semantic"
    assert matched_only_with_semantic.score.source == "rank_fusion"
    assert matched_only_with_semantic.score.type == "rrf"

    # Check that invalid semantic model is rejected
    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/retrieve",
        json={
            "query": {
                "semantic": {
                    "query": Q,
                    "vectorset": "non-existing-model",
                },
            },
        },
    )
    assert resp.status_code == 422
    json_body = resp.json()
    assert (
        json_body["detail"]
        == "Invalid query. Error in vectorset: Vectorset non-existing-model doesn't exist in your Knowledge Box"
    )

    # Test that matryoshka dimension is enforced
    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/retrieve",
        json={
            "query": {
                "semantic": {
                    "query": Q[:50],  # too short
                    "vectorset": "my-semantic-model",
                },
            },
        },
    )
    assert resp.status_code == 422
    json_body = resp.json()
    assert (
        json_body["detail"]
        == "Invalid query. Error in vector: Invalid vector length, please check valid embedding size for my-semantic-model model"
    )


@pytest.mark.deploy_modes("standalone")
async def test_retrieve_with_reranking(
    nucliadb_search: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox: str,
) -> None:
    kbid = knowledgebox
    rid = await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/retrieve",
        json=RetrievalRequest(
            query=RawQuery(
                keyword=KeywordQuery(
                    query="smb wonder",
                    min_score=0.0,
                ),
                semantic=SemanticQuery(
                    query=Q,
                    vectorset="my-semantic-model",
                    min_score=-1,
                ),
            ),
            top_k=10,
            reranker=RerankerName.PREDICT_RERANKER,
        ).model_dump(),
    )
    assert resp.status_code == 200

    body = RetrievalResponse.model_validate(resp.json())

    # we expect to match all paragraphs in the resource
    paragraph_ids = [match.id for match in body.matches]
    expected_paragraph_ids = [
        f"{rid}/a/title/0-24",
        f"{rid}/a/summary/0-44",
        f"{rid}/f/smb-wonder/0-99",
        f"{rid}/f/smb-wonder/99-145",
        f"{rid}/f/smb-wonder/145-234",
    ]
    assert sorted(paragraph_ids) == sorted(expected_paragraph_ids)

    # all matches have been reranked
    score = len(body.matches) - 1  # scores start with 0
    for match in body.matches:
        assert match.score.history[-1].source == "reranker"
        assert match.score.history[-1].type == "reranker"
        assert match.score.history[-1].score == score
        score -= 1  # dummy reranker gives index numbers as scores

    # this paragraph has no keyord match, so we matched it due to semantic
    matched_only_with_semantic = next(
        filter(
            lambda match: match.id == f"{rid}/f/smb-wonder/145-234",
            body.matches,
        )
    )
    assert len(matched_only_with_semantic.score.history) == 3
    assert matched_only_with_semantic.score.history[0].source == "index"
    assert matched_only_with_semantic.score.history[0].type == "semantic"
    assert matched_only_with_semantic.score.history[1].source == "rank_fusion"
    assert matched_only_with_semantic.score.history[1].type == "rrf"
    assert matched_only_with_semantic.score.source == "reranker"
    assert matched_only_with_semantic.score.type == "reranker"

    resp = await nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/retrieve",
        json=RetrievalRequest(
            query=RawQuery(
                keyword=KeywordQuery(
                    query="smb wonder",
                    min_score=0.0,
                ),
                semantic=SemanticQuery(
                    query=Q,
                    vectorset="my-semantic-model",
                    min_score=-1,
                ),
            ),
            top_k=2,
            reranker=PredictReranker(window=10),
        ).model_dump(),
    )
    assert resp.status_code == 200

    body = RetrievalResponse.model_validate(resp.json())

    # only the top_k have been returned, but the reranker has reranked them all.
    # Thus, their score reflect this fact
    paragraph_ids = [match.id for match in body.matches]
    expected_paragraph_ids = [
        f"{rid}/a/title/0-24",
        f"{rid}/f/smb-wonder/145-234",
    ]
    assert sorted(paragraph_ids) == sorted(expected_paragraph_ids)

    score = 5 - 1  # 5 total results, starting scores from 0
    for match in body.matches:
        assert match.score.history[-1].source == "reranker"
        assert match.score.history[-1].type == "reranker"
        assert match.score.history[-1].score == score
        score -= 1


@pytest.mark.deploy_modes("standalone")
async def test_retrieve_query_parsing() -> None:
    """Validate /retrieve query parsing for raw and complex queries."""

    kbid = "kbid"

    # same but without validation, so we can inject the Fetcher mock
    def MockedParsedQuery(*args, **kwargs) -> ParsedQuery:
        return ParsedQuery.model_construct(*args, **kwargs)

    fetcher = AsyncMock()
    fetcher.get_vectorset.return_value = "my-vectorset"
    fetcher.get_user_vectorset = AsyncMock(return_value="my-vectorset")
    fetcher.get_query_vector = AsyncMock(return_value=Q)
    fetcher.get_matryoshka_dimension = AsyncMock(return_value=128)

    synonyms = knowledgebox_pb2.Synonyms()
    synonyms.terms["wonder"].synonyms.extend(["awe", "fascination"])
    fetcher.get_synonyms = AsyncMock(return_value=synonyms)

    Fetcher = Mock(return_value=fetcher)

    with (
        patch("nucliadb.search.search.query_parser.parsers.retrieve.ParsedQuery", new=MockedParsedQuery),
        patch("nucliadb.search.search.query_parser.parsers.retrieve.Fetcher", new=Fetcher),
        patch(
            "nucliadb.search.search.query_parser.parsers.retrieve.filter_hidden_resources",
            return_value=False,
        ),
        patch(
            "nucliadb.search.search.query_parser.parsers.retrieve.kb_security_enforced",
            return_value=None,
        ),
    ):
        # TEST: textual query

        parsed = await parse_retrieve(
            kbid,
            RetrievalRequest(
                query=Query(
                    query="smb wonder",
                ),
                top_k=10,
            ),
        )
        assert parsed.retrieval.query.keyword is not None
        assert parsed.retrieval.query.keyword.query == "smb wonder"
        assert parsed.retrieval.query.semantic is not None
        assert parsed.retrieval.query.semantic.query == Q[:128]
        assert parsed.retrieval.query.semantic.vectorset == "my-vectorset"
        assert parsed.retrieval.top_k == 10

        # TEST: disable keyword search

        parsed = await parse_retrieve(
            kbid,
            RetrievalRequest(
                query=Query(
                    query="smb wonder",
                    override=QueryOverrides(
                        keyword="disabled",
                    ),
                ),
                top_k=10,
            ),
        )
        assert parsed.retrieval.query.keyword is None
        assert parsed.retrieval.query.semantic is not None

        # TEST: disable semantic search

        parsed = await parse_retrieve(
            kbid,
            RetrievalRequest(
                query=Query(
                    query="smb wonder",
                    override=QueryOverrides(
                        semantic="disabled",
                    ),
                ),
                top_k=10,
            ),
        )
        assert parsed.retrieval.query.keyword is not None
        assert parsed.retrieval.query.semantic is None

        # TEST: disable all indexes (not valid)

        with pytest.raises(ValidationError):
            await parse_retrieve(
                kbid,
                RetrievalRequest(
                    query=Query(
                        query="smb wonder",
                        override=QueryOverrides(
                            keyword="disabled",
                            semantic="disabled",
                            graph="disabled",
                        ),
                    ),
                    top_k=10,
                ),
            )

        # TEST: override keyword search

        parsed = await parse_retrieve(
            kbid,
            RetrievalRequest(
                query=Query(
                    query="smb wonder",
                    override=QueryOverrides(
                        keyword=KeywordOverrides(
                            min_score=0.7,
                            with_synonyms=True,
                        )
                    ),
                ),
                top_k=10,
            ),
        )
        assert parsed.retrieval.query.keyword is not None
        assert parsed.retrieval.query.keyword.min_score == 0.7
        assert parsed.retrieval.query.keyword.is_synonyms_query is True

        # TEST: rephrase

        parsed = await parse_retrieve(
            kbid,
            RetrievalRequest(
                query=Query(
                    query="smb wonder",
                ),
                top_k=10,
            ),
        )
        assert Fetcher.call_args.kwargs["rephrase"] is False
        assert Fetcher.call_args.kwargs["rephrase_prompt"] is None

        parsed = await parse_retrieve(
            kbid,
            RetrievalRequest(
                query=Query(
                    query="smb wonder",
                    rephrase=True,
                ),
                top_k=10,
            ),
        )
        assert Fetcher.call_args.kwargs["rephrase"] is True
        assert Fetcher.call_args.kwargs["rephrase_prompt"] is None

        parsed = await parse_retrieve(
            kbid,
            RetrievalRequest(
                query=Query(query="smb wonder", rephrase=Rephrase(prompt="my custom prompt")),
                top_k=10,
            ),
        )
        assert Fetcher.call_args.kwargs["rephrase"] is True
        assert Fetcher.call_args.kwargs["rephrase_prompt"] == "my custom prompt"

        # TEST: query image

        parsed = await parse_retrieve(
            kbid,
            RetrievalRequest(
                query=Query(
                    query="smb wonder",
                    image=None,
                ),
                top_k=10,
            ),
        )
        assert Fetcher.call_args.kwargs["query_image"] is None

        img = Image(content_type="image/png", b64encoded="b64-content")
        parsed = await parse_retrieve(
            kbid,
            RetrievalRequest(
                query=Query(query="smb wonder", image=img),
                top_k=10,
            ),
        )
        assert Fetcher.call_args.kwargs["query_image"] == img


@pytest.mark.deploy_modes("standalone")
async def test_retrieve_with_kv_field_filter(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    clothing_store = await clothing_store_resources(kbid, nucliadb_writer, nucliadb_ingest_grpc)
    product_name_by_rid = {product_name: rid for rid, product_name in clothing_store.items()}

    async def retrieve_with_kv_filter(filter: dict) -> set:
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/retrieve",
            json={
                "query": {"keyword": {"query": ""}},
                "filters": {
                    "filter_expression": {
                        "key_value": filter,
                    },
                },
            },
        )
        assert resp.status_code == 200, resp.text
        body = RetrievalResponse.model_validate_json(resp.text)

        products = set()
        for match in body.matches:
            paragraph_id = ParagraphId.from_string(match.id)
            rid = paragraph_id.rid
            product_name = product_name_by_rid[rid]
            products.add(product_name)
        return products

    products = await retrieve_with_kv_filter(
        {
            "field_id": "product",
            "key": "color",
            "op": "exact_match",
            "value": "white",
        }
    )
    assert products == {"floral-skirt", "white-t-shirt"}

    products = await retrieve_with_kv_filter(
        {"field_id": "product", "key": "price", "op": "range", "gte": 20.0}
    )
    assert products == {"floral-skirt"}

    products = await retrieve_with_kv_filter(
        {"field_id": "product", "key": "price", "op": "range", "lte": 20.0}
    )
    assert products == {"white-t-shirt"}
