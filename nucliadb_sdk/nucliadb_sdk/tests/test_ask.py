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

import unittest
import unittest.mock

import nucliadb_sdk
from nucliadb_models.metadata import RelationType
from nucliadb_models.search import (
    AnswerAskResponseItem,
    AskResponseItem,
    AskTimings,
    AskTokens,
    CitationsAskResponseItem,
    DirectionalRelation,
    EntitySubgraph,
    EntityType,
    KnowledgeboxFindResults,
    MaxTokens,
    MetadataAskResponseItem,
    RelationDirection,
    Relations,
    RelationsAskResponseItem,
    RetrievalAskResponseItem,
    StatusAskResponseItem,
    SyncAskResponse,
)
from nucliadb_sdk.v2.sdk import ask_response_parser


def test_ask_on_kb(docs_dataset, sdk: nucliadb_sdk.NucliaDB):
    result: SyncAskResponse = sdk.ask(
        kbid=docs_dataset,
        query="Nuclia loves Semantic Search",
        generative_model="everest",
        prompt="Given this context: {context}. Answer this {question} in a concise way using the provided context",
        extra_context=[
            "Nuclia is a powerful AI search platform",
            "AI Search involves semantic search",
        ],
        # Control the number of AI tokens used for every request
        max_tokens=MaxTokens(context=100, answer=50),
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
    _ = sdk.ask_on_resource(
        kbid=docs_dataset, rid=rid, query="Nuclia loves Semantic Search"
    )

    # Check askting with the whole resource (no retrieval)
    _ = sdk.ask_on_resource(
        kbid=docs_dataset,
        rid=rid,
        query="Nuclia loves Semantic Search",
        rag_strategies=[{"name": "full_resource"}],
    )


def test_ask_response_parser():
    items = [
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
                                relation=RelationType.ABOUT,
                                relation_label="performing",
                                direction=RelationDirection.OUT,
                            )
                        ]
                    )
                }
            )
        ),
        RetrievalAskResponseItem(results=KnowledgeboxFindResults(resources={})),
        MetadataAskResponseItem(
            tokens=AskTokens(input=10, output=5),
            timings=AskTimings(generative_first_chunk=0.1, generative_total=0.2),
        ),
        CitationsAskResponseItem(citations={"some/paragraph/id": "This is a citation"}),
    ]
    raw_lines = [AskResponseItem(item=item).json() for item in items]
    response = unittest.mock.Mock()
    response.headers = {"NUCLIA-LEARNING-ID": "learning_id"}
    response.iter_lines = unittest.mock.Mock(return_value=raw_lines)

    ask_response: SyncAskResponse = ask_response_parser(response)

    assert ask_response.learning_id == "learning_id"
    assert ask_response.answer == "This is your Nuclia answer."
    assert ask_response.status == "success"
    assert (
        ask_response.relations.entities["Nuclia"].related_to[0].entity
        == "Semantic Search"
    )
    assert ask_response.citations["some/paragraph/id"] == "This is a citation"
    assert ask_response.retrieval_results.resources == {}
    assert ask_response.metadata.tokens.input == 10
    assert ask_response.metadata.tokens.output == 5
    assert ask_response.metadata.timings.generative_first_chunk == 0.1
    assert ask_response.metadata.timings.generative_total == 0.2
