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
from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient

from nucliadb_models.search import SyncAskResponse
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature

if has_feature(const.Features.ASK_DECOUPLED):
    from nucliadb.search.search import graph_strategy

    MODULE = "nucliadb.search.search.graph_strategy"
else:
    from nucliadb.search.search import old_graph_strategy as graph_strategy  # type: ignore[no-redef]

    MODULE = "nucliadb.search.search.old_graph_strategy"


@pytest.mark.parametrize("relation_ranking", ["generative", "reranker"])
@patch(f"{MODULE}.rank_relations_reranker")
@patch(f"{MODULE}.rank_relations_generative")
@pytest.mark.deploy_modes("standalone")
async def test_ask_graph_strategy(
    mocker_generative,
    mocker_reranker,
    relation_ranking: str,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource,
    dummy_predict,
):
    # Mock the rank_relations functions to return the same relations with a score of 5 (no ranking)
    # This functions are unit tested and require connection to predict
    def mock_rank(relations, *args, **kwargs):
        return relations, {ent: [5 for _ in rels.related_to] for ent, rels in relations.entities.items()}

    mocker_generative.side_effect = mock_rank
    mocker_reranker.side_effect = mock_rank

    data = {
        "query": "Which actors have been in movies directed by Christopher Nolan?",
        "rag_strategies": [
            {
                "name": "graph_beta",
                "hops": 2,
                "top_k": 5,
                "agentic_graph_only": False,
                "query_entity_detection": "suggest",
                "relation_ranking": relation_ranking,
                "relation_text_as_paragraphs": False,
            }
        ],
        "reranker": "noop",
        "debug": True,
    }
    headers = {"X-Synchronous": "True"}

    url = f"/kb/{standalone_knowledgebox}/ask"

    async def assert_ask(d, expected_paragraphs_text, expected_paragraphs_relations):
        resp = await nucliadb_reader.post(
            url,
            json=d,
            headers=headers,
        )
        assert resp.status_code == 200, resp.text
        ask_response = SyncAskResponse.model_validate_json(resp.content)
        assert ask_response.status == "success"

        paragraphs = ask_response.prequeries["graph"].resources[graph_resource].fields
        paragraph_texts = {
            p_id: paragraph.text
            for p_id, field in paragraphs.items()
            for paragraph in field.paragraphs.values()
        }
        assert paragraph_texts == expected_paragraphs_text
        paragraph_relations = {
            p_id: [
                {ent, r.relation_label, r.entity}
                for ent, rels in paragraph.relevant_relations.entities.items()
                for r in rels.related_to
            ]
            for p_id, field in paragraphs.items()
            for paragraph in field.paragraphs.values()
            if paragraph.relevant_relations is not None
        }
        assert paragraph_relations == expected_paragraphs_relations

        paragraph_scores = [
            paragraph.score for field in paragraphs.values() for paragraph in field.paragraphs.values()
        ]
        assert all(score == 5 for score in paragraph_scores)

        # We expect a ranking for each hop
        assert mocker_reranker.call_count == 2 or mocker_generative.call_count == 2
        mocker_reranker.reset_mock()
        mocker_generative.reset_mock()

    expected_paragraphs_text = {
        "/t/inception3": "Joseph Gordon-Levitt starred in Inception.",
        "/t/inception2": "Leonardo DiCaprio starred in Inception.",
        "/t/inception1": "Christopher Nolan directed Inception.",
    }
    expected_paragraphs_relations = {
        "/t/inception1": [{"Christopher Nolan", "directed", "Inception"}],
        "/t/inception2": [{"Leonardo DiCaprio", "starred", "Inception"}],
        "/t/inception3": [{"Joseph Gordon-Levitt", "starred", "Inception"}],
    }
    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)

    data["query"] = "In which movie has DiCaprio starred? And Joseph Gordon-Levitt?"
    expected_paragraphs_text = {
        "/t/inception1": "Christopher Nolan directed Inception.",
        "/t/inception3": "Joseph Gordon-Levitt starred in Inception.",
        "/t/inception2": "Leonardo DiCaprio starred in Inception.",
        "/t/leo": "Leonardo DiCaprio is a great actor. DiCaprio started acting in 1989.",
    }
    expected_paragraphs_relations["/t/leo"] = [{"Leonardo DiCaprio", "analogy", "DiCaprio"}]
    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)

    # Setup a mock to test query entity extraction with predict
    dummy_predict.detect_entities = AsyncMock(
        return_value=[
            RelationNode(
                value="DiCaprio",
                ntype=RelationNode.NodeType.ENTITY,
                subtype="ACTOR",
            ),
            RelationNode(
                value="Joseph Gordon-Levitt",
                ntype=RelationNode.NodeType.ENTITY,
                subtype="ACTOR",
            ),
        ]
    )

    # Run the same query but with query_entity_detection set to "predict"
    data["rag_strategies"][0]["query_entity_detection"] = "predict"  # type: ignore

    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)

    # Now test with relation_text_as_paragraphs
    data["rag_strategies"][0]["relation_text_as_paragraphs"] = True  # type: ignore
    expected_paragraphs_text = {
        "/t/inception2": "- Leonardo DiCaprio starred Inception",
        "/t/leo": "- Leonardo DiCaprio analogy DiCaprio",
        "/t/inception1": "- Christopher Nolan directed Inception",
        "/t/inception3": "- Joseph Gordon-Levitt starred Inception",
    }
    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)

    # Now with agentic graph only
    data["rag_strategies"][0]["agentic_graph_only"] = True  # type: ignore
    data["rag_strategies"][0]["relation_text_as_paragraphs"] = False  # type: ignore
    expected_paragraphs_text = {
        "/t/inception2": "Leonardo DiCaprio starred in Inception.",
        "/t/leo": "Leonardo DiCaprio is a great actor. DiCaprio started acting in 1989.",
    }
    del expected_paragraphs_relations["/t/inception1"]
    del expected_paragraphs_relations["/t/inception3"]
    await assert_ask(data, expected_paragraphs_text, expected_paragraphs_relations)


@patch(f"{MODULE}.rank_relations_reranker")
@patch(f"{MODULE}.rank_relations_generative")
@pytest.mark.deploy_modes("standalone")
async def test_ask_graph_strategy_with_user_relations(
    mocker_generative,
    mocker_reranker,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    dummy_predict,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledgegraph",
            "usermetadata": {
                "relations": [
                    {
                        "relation": "ENTITY",
                        "from": {"type": "entity", "group": "DIRECTOR", "value": "Christopher Nolan"},
                        "to": {"type": "entity", "group": "MOVIE", "value": "Inception"},
                        "label": "directed",
                    },
                    {
                        "relation": "ENTITY",
                        "from": {"type": "entity", "group": "ACTOR", "value": "Leonardo DiCaprio"},
                        "to": {"type": "entity", "group": "MOVIE", "value": "Inception"},
                        "label": "starred",
                    },
                    {
                        "relation": "ENTITY",
                        "from": {"type": "entity", "group": "ACTOR", "value": "Joseph Gordon-Levitt"},
                        "to": {"type": "entity", "group": "MOVIE", "value": "Inception"},
                        "label": "starred",
                    },
                    {
                        "relation": "ENTITY",
                        "from": {"type": "entity", "group": "ACTOR", "value": "Leonardo DiCaprio"},
                        "to": {"type": "entity", "group": "ACTOR", "value": "DiCaprio"},
                        "label": "analogy",
                    },
                ]
            },
        },
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Mock the rank_relations functions to return the same relations with a score of 5 (no ranking)
    # This functions are unit tested and require connection to predict
    def mock_rank(relations, *args, **kwargs):
        return relations, {ent: [5 for _ in rels.related_to] for ent, rels in relations.entities.items()}

    mocker_generative.side_effect = mock_rank
    mocker_reranker.side_effect = mock_rank

    # With relation_text_as_paragraphs=False, cannot return user relations (no paragraph)
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "Which actors have been in movies directed by Christopher Nolan?",
            "rag_strategies": [
                {
                    "name": "graph_beta",
                    "hops": 2,
                    "top_k": 5,
                    "agentic_graph_only": False,
                    "query_entity_detection": "suggest",
                    "relation_ranking": "reranker",
                    "relation_text_as_paragraphs": False,
                }
            ],
            "reranker": "noop",
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert ask_response.status == "no_retrieval_data"

    # With relation_text_as_paragraphs=True, should answer the question
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "Which actors have been in movies directed by Christopher Nolan?",
            "rag_strategies": [
                {
                    "name": "graph_beta",
                    "hops": 2,
                    "top_k": 5,
                    "agentic_graph_only": False,
                    "query_entity_detection": "suggest",
                    "relation_ranking": "reranker",
                    "relation_text_as_paragraphs": True,
                }
            ],
            "reranker": "noop",
            "debug": True,
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200, resp.text
    ask_response = SyncAskResponse.model_validate_json(resp.content)
    assert ask_response.status == "success"

    fields = ask_response.prequeries["graph"].resources[rid].fields  # type: ignore
    assert list(fields.keys()) == ["/a/usermetadata"]

    paragraph_texts = {
        p_id: paragraph.text for p_id, paragraph in fields["/a/usermetadata"].paragraphs.items()
    }
    assert sorted(list(paragraph_texts.values())) == [
        "- Christopher Nolan directed Inception",
        "- Joseph Gordon-Levitt starred Inception",
        # "- Leonardo DiCaprio analogy DiCaprio", # Only with 3+ hops
        "- Leonardo DiCaprio starred Inception",
    ]


@pytest.mark.deploy_modes("standalone")
async def test_ask_graph_strategy_inner_fuzzy_prefix_search(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource,
    dummy_predict,
):
    kbid = standalone_knowledgebox

    related = await graph_strategy.fuzzy_search_entities(
        kbid, "Which actors have been in movies directed by Christopher Nolan?"
    )
    assert related is not None
    assert related.total == 1 == len(related.entities)
    related.entities[0].value == "Christopher Nolan"
    related.entities[0].family == "DIRECTOR"

    related = await graph_strategy.fuzzy_search_entities(
        kbid, "Did Leonard and Joseph perform in the same film?"
    )
    assert related is not None
    assert related.total == 2 == len(related.entities)
    assert {(r.value, r.family) for r in related.entities} == {
        ("Leonardo DiCaprio", "ACTOR"),
        ("Joseph Gordon-Levitt", "ACTOR"),
    }
