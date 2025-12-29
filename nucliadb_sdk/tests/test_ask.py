# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import unittest.mock

import pytest

import nucliadb_sdk
from nucliadb_models.metadata import RelationType
from nucliadb_models.search import (
    AnswerAskResponseItem,
    AskResponseItem,
    AskTimings,
    AskTokens,
    CitationsAskResponseItem,
    ConsumptionResponseItem,
    DirectionalRelation,
    EntitySubgraph,
    EntityType,
    KnowledgeboxFindResults,
    MaxTokens,
    MetadataAskResponseItem,
    ReasoningAskResponseItem,
    RelationDirection,
    Relations,
    RelationsAskResponseItem,
    RetrievalAskResponseItem,
    StatusAskResponseItem,
    SyncAskResponse,
)
from nucliadb_sdk.v2.sdk import ask_response_parser, ask_response_parser_async


def test_ask_on_kb(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    result: SyncAskResponse = sdk.ask(
        kbid=docs_dataset,
        query="Nuclia loves Semantic Search",
        features=["keyword", "semantic", "relations"],
        generative_model="everest",
        prompt="Given this context: {context}. Answer this {question} in a concise way using the provided context",
        extra_context=[
            "Nuclia is a powerful AI search platform",
            "AI Search involves semantic search",
        ],
        # Control the number of AI tokens used for every request
        max_tokens=MaxTokens(context=100, answer=50),
        answer_json_schema={
            "type": "object",
            "properties": {
                "answer": {"type": "string"},
                "confidence": {"type": "number"},
            },
        },
        top_k=20,
        rank_fusion="rrf",
        reranker="noop",
        headers={"any-header": "any-value"},
    )
    assert result.learning_id == "00"
    assert result.answer == "valid answer to"
    assert len(result.retrieval_results.resources) == 7
    assert result.relations


def test_ask_on_kb_with_citations(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    result = sdk.ask(
        kbid=docs_dataset,
        query="Nuclia loves Semantic Search",
        citations=True,
    )
    assert result.citations == {}


def test_ask_on_kb_no_context_found(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    result = sdk.ask(kbid=docs_dataset, query="penguin")
    assert result.answer == "Not enough data to answer this."


def test_ask_on_resource(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    rid = sdk.list_resources(kbid=docs_dataset).resources[0].id
    # With retrieval
    _ = sdk.ask_on_resource(kbid=docs_dataset, rid=rid, query="Nuclia loves Semantic Search")

    # Check askting with the whole resource (no retrieval)
    _ = sdk.ask_on_resource(
        kbid=docs_dataset,
        rid=rid,
        query="Nuclia loves Semantic Search",
        rag_strategies=[{"name": "full_resource"}],
    )


def test_ask_response_parser_stream():
    items = [
        ReasoningAskResponseItem(text="test"),
        ReasoningAskResponseItem(text=" reasoning."),
        AnswerAskResponseItem(text="This is"),
        AnswerAskResponseItem(text=" your Nuclia answer."),
        StatusAskResponseItem(code="0", status="success"),
        RelationsAskResponseItem(
            relations=Relations(
                entities={
                    "Nuclia": EntitySubgraph(
                        related_to=[
                            DirectionalRelation(
                                entity="Semantic Search",
                                entity_type=EntityType.ENTITY,
                                entity_subtype="concept",
                                relation=RelationType.ABOUT,
                                relation_label="performing",
                                direction=RelationDirection.OUT,
                                resource_id="resource_id",
                            )
                        ]
                    )
                }
            )
        ),
        RetrievalAskResponseItem(results=KnowledgeboxFindResults(resources={})),
        MetadataAskResponseItem(
            tokens=AskTokens(input=10, output=5, input_nuclia=0.01, output_nuclia=0.005),
            timings=AskTimings(generative_first_chunk=0.1, generative_total=0.2),
        ),
        CitationsAskResponseItem(citations={"some/paragraph/id": "This is a citation"}),
        ConsumptionResponseItem.model_validate(
            {
                "normalized_tokens": {"input": 1, "output": 2, "image": 3},
                "customer_key_tokens": {"input": 4, "output": 5, "image": 6},
            }
        ),
    ]
    raw_lines = [AskResponseItem(item=item).model_dump_json() for item in items]
    response = unittest.mock.Mock()
    response.headers = {
        "NUCLIA-LEARNING-ID": "learning_id",
        "Content-Type": "application/x-ndjson",
    }
    response.iter_lines = unittest.mock.Mock(return_value=raw_lines)

    ask_response = ask_response_parser(response)

    assert ask_response.learning_id == "learning_id"
    assert ask_response.answer == "This is your Nuclia answer."
    assert ask_response.reasoning == "test reasoning."
    assert ask_response.status == "success"
    assert ask_response.relations.entities["Nuclia"].related_to[0].entity == "Semantic Search"
    assert ask_response.relations.entities["Nuclia"].related_to[0].entity_subtype == "concept"
    assert ask_response.citations["some/paragraph/id"] == "This is a citation"
    assert ask_response.retrieval_results.resources == {}
    assert ask_response.metadata.tokens.input == 10
    assert ask_response.metadata.tokens.output == 5
    assert ask_response.metadata.tokens.input_nuclia == 0.01
    assert ask_response.metadata.tokens.output_nuclia == 0.005
    assert ask_response.metadata.timings.generative_first_chunk == 0.1
    assert ask_response.metadata.timings.generative_total == 0.2
    assert ask_response.consumption.normalized_tokens.input == 1
    assert ask_response.consumption.normalized_tokens.output == 2
    assert ask_response.consumption.normalized_tokens.image == 3
    assert ask_response.consumption.customer_key_tokens.input == 4
    assert ask_response.consumption.customer_key_tokens.output == 5
    assert ask_response.consumption.customer_key_tokens.image == 6


async def test_ask_response_parser_async_stream():
    items = [
        ReasoningAskResponseItem(text="test"),
        ReasoningAskResponseItem(text=" reasoning."),
        AnswerAskResponseItem(text="This is"),
        AnswerAskResponseItem(text=" your Nuclia answer."),
        StatusAskResponseItem(code="0", status="success"),
        RelationsAskResponseItem(
            relations=Relations(
                entities={
                    "Nuclia": EntitySubgraph(
                        related_to=[
                            DirectionalRelation(
                                entity="Semantic Search",
                                entity_type=EntityType.ENTITY,
                                entity_subtype="concept",
                                relation=RelationType.ABOUT,
                                relation_label="performing",
                                direction=RelationDirection.OUT,
                                resource_id="resource_id",
                            )
                        ]
                    )
                }
            )
        ),
        RetrievalAskResponseItem(results=KnowledgeboxFindResults(resources={})),
        MetadataAskResponseItem(
            tokens=AskTokens(input=10, output=5, input_nuclia=0.01, output_nuclia=0.005),
            timings=AskTimings(generative_first_chunk=0.1, generative_total=0.2),
        ),
        CitationsAskResponseItem(citations={"some/paragraph/id": "This is a citation"}),
    ]

    async def raw_lines_iterator():
        raw_lines = [AskResponseItem(item=item).model_dump_json() for item in items]
        for line in raw_lines:
            yield line

    response = unittest.mock.Mock()
    response.headers = {
        "NUCLIA-LEARNING-ID": "learning_id",
        "Content-Type": "application/x-ndjson",
    }
    response.aiter_lines = unittest.mock.Mock(return_value=raw_lines_iterator())

    ask_response = await ask_response_parser_async(response)

    assert ask_response.learning_id == "learning_id"
    assert ask_response.answer == "This is your Nuclia answer."
    assert ask_response.reasoning == "test reasoning."
    assert ask_response.status == "success"
    assert ask_response.relations.entities["Nuclia"].related_to[0].entity == "Semantic Search"
    assert ask_response.relations.entities["Nuclia"].related_to[0].entity_subtype == "concept"
    assert ask_response.citations["some/paragraph/id"] == "This is a citation"
    assert ask_response.retrieval_results.resources == {}
    assert ask_response.metadata.tokens.input == 10
    assert ask_response.metadata.tokens.output == 5
    assert ask_response.metadata.tokens.input_nuclia == 0.01
    assert ask_response.metadata.tokens.output_nuclia == 0.005
    assert ask_response.metadata.timings.generative_first_chunk == 0.1
    assert ask_response.metadata.timings.generative_total == 0.2


def test_ask_synchronous(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    sdk.session.headers["X-Synchronous"] = "true"
    resp = sdk.ask(
        kbid=docs_dataset,
        query="Nuclia loves Semantic Search",
    )
    assert isinstance(resp, SyncAskResponse)
    sdk.session.headers.pop("X-Synchronous", None)


def test_ask_stream(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    sdk.session.headers["X-Synchronous"] = "false"
    resp = sdk.ask(
        kbid=docs_dataset,
        query="Nuclia loves Semantic Search",
    )
    assert isinstance(resp, SyncAskResponse)
    sdk.session.headers.pop("X-Synchronous", None)


@pytest.mark.parametrize(
    "rag_strategies",
    [
        [{"name": "full_resource"}],
        [{"name": "neighbouring_paragraphs", "before": 1, "after": 1}],
        [{"name": "hierarchy", "count": 40}],
        [{"name": "field_extension", "fields": ["a/title", "a/summary"]}],
        [
            {
                "name": "metadata_extension",
                "types": ["origin", "classification_labels", "ners", "extra_metadata"],
            }
        ],
        [
            {
                "name": "prequeries",
                "queries": [
                    {"request": {"query": "Nuclia loves Semantic Search"}, "weight": 1.0},
                    {"request": {"query": "Nuclia is a powerful AI search platform"}, "weight": 3.0},
                ],
            }
        ],
    ],
)
def test_ask_rag_strategies(docs_dataset, sdk: nucliadb_sdk.NucliaDB, rag_strategies):
    sdk.ask(
        kbid=docs_dataset, query="Does Nuclia offer RAG as a service?", rag_strategies=rag_strategies
    )


def test_ask_rag_strategy_model(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    from nucliadb_models.search import FullResourceStrategy

    rag_strategy = FullResourceStrategy()
    sdk.ask(
        kbid=docs_dataset, query="Does Nuclia offer RAG as a service?", rag_strategies=[rag_strategy]
    )
