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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from nuclia_models.predict.generative_responses import (
    JSONGenerativeResponse,
    MetaGenerativeResponse,
    StatusGenerativeResponse,
)

from nucliadb.search.search.graph_strategy import (
    get_paragraph_info_from_relations,
    rank_relations_generative,
    rank_relations_reranker,
)
from nucliadb_models import RelationMetadata
from nucliadb_models.internal.predict import (
    RerankResponse,
)
from nucliadb_models.metadata import RelationType
from nucliadb_models.search import (
    DirectionalRelation,
    EntitySubgraph,
    EntityType,
    RelationDirection,
    Relations,
)


@pytest.mark.asyncio
@patch("nucliadb.search.search.graph_strategy.get_predict")
async def test_rank_relations_generative(
    mocker,
):
    mock_json_response = {
        "triplets": [
            {
                "head_entity": "John Adams Roofing Inc.",
                "relationship": "has product",
                "tail_entity": "Socks",
                "score": 2,
            },
            {
                "head_entity": "John Adams Roofing Inc.",
                "relationship": "has product",
                "tail_entity": "Titanium Grade 3 Roofing Nails",
                "score": 10,
            },
            {
                "head_entity": "John Adams Roofing Inc.",
                "relationship": "is located in",
                "tail_entity": "New York",
                "score": 6,
            },
            {
                "head_entity": "John Adams",
                "relationship": "married to",
                "tail_entity": "Abigail Adams",
                "score": 0,
            },
        ]
    }
    chat_mock = AsyncMock()
    chat_mock.__aiter__.return_value = iter(
        [
            MagicMock(chunk=JSONGenerativeResponse(object=mock_json_response)),
            MagicMock(chunk=StatusGenerativeResponse(code="0")),
            MagicMock(chunk=MetaGenerativeResponse(input_tokens=10, output_tokens=5, timings={})),
        ]
    )
    predict_mock = AsyncMock()
    predict_mock.chat_query_ndjson.return_value = ("my_ident", "fake_llm", chat_mock)
    mocker.return_value = predict_mock
    relations = Relations(
        entities={
            "John Adams Roofing Inc.": EntitySubgraph(
                related_to=[
                    DirectionalRelation(
                        entity="Socks",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PRODUCT",
                        relation_label="has product",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    ),
                    DirectionalRelation(
                        entity="Titanium Grade 3 Roofing Nails",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PRODUCT",
                        relation_label="has product",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    ),
                    DirectionalRelation(
                        entity="New York",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="LOCATION",
                        relation_label="is located in",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    ),
                    DirectionalRelation(
                        entity="New York",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PLACE",
                        relation_label="is located in",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    ),
                ]
            ),
            "John Adams": EntitySubgraph(
                related_to=[
                    DirectionalRelation(
                        entity="Abigail Adams",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PERSON",
                        relation_label="married to",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    )
                ]
            ),
        }
    )
    result, scores = await rank_relations_generative(
        relations=relations, query="my_query", kbid="my_kbid", user="my_user", top_k=2, score_threshold=0
    )
    assert "John Adams Roofing Inc." in result.entities
    assert (
        result.entities["John Adams Roofing Inc."].related_to
        == relations.entities["John Adams Roofing Inc."].related_to[1:]
    )
    assert "John Adams" not in result.entities
    assert dict(scores) == {
        "John Adams Roofing Inc.": [10, 6, 6],
    }


@pytest.mark.asyncio
@patch("nucliadb.search.search.graph_strategy.get_predict")
async def test_rank_relations_reranker(
    mocker,
):
    mock_rerank_response = RerankResponse(context_scores={"0": 2, "1": 10, "2": 6, "3": 0})
    predict_mock = AsyncMock()
    predict_mock.rerank.return_value = mock_rerank_response
    mocker.return_value = predict_mock
    relations = Relations(
        entities={
            "John Adams Roofing Inc.": EntitySubgraph(
                related_to=[
                    DirectionalRelation(
                        entity="Socks",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PRODUCT",
                        relation_label="has product",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    ),
                    DirectionalRelation(
                        entity="Titanium Grade 3 Roofing Nails",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PRODUCT",
                        relation_label="has product",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    ),
                    DirectionalRelation(
                        entity="New York",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="LOCATION",
                        relation_label="is located in",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    ),
                    DirectionalRelation(
                        entity="New York",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PLACE",
                        relation_label="is located in",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    ),
                ]
            ),
            "John Adams": EntitySubgraph(
                related_to=[
                    DirectionalRelation(
                        entity="Abigail Adams",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PERSON",
                        relation_label="married to",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                        resource_id="resource_uuid",
                    )
                ]
            ),
        }
    )
    result, scores = await rank_relations_reranker(
        relations=relations, query="my_query", kbid="my_kbid", user="my_user", top_k=2, score_threshold=0
    )
    assert "John Adams Roofing Inc." in result.entities
    assert (
        result.entities["John Adams Roofing Inc."].related_to
        == relations.entities["John Adams Roofing Inc."].related_to[1:]
    )
    assert "John Adams" not in result.entities
    assert dict(scores) == {
        "John Adams Roofing Inc.": [10, 6, 6],
    }


def test_get_paragraph_info_from_relations():
    entity_name = "entityA"
    paragraph_id_str = "blabla/u/link/10-20"
    relation = DirectionalRelation(
        entity="entityB",
        entity_type=EntityType.ENTITY,
        entity_subtype="t",
        relation_label="bla",
        relation=RelationType.ENTITY,
        direction=RelationDirection.OUT,
        metadata=RelationMetadata(paragraph_id=paragraph_id_str),
        resource_id="blabla",
    )

    relations = Relations(entities={entity_name: EntitySubgraph(related_to=[relation])})

    # Scores for entityA: single float in a list
    scores = {entity_name: [0.8]}

    result = get_paragraph_info_from_relations(relations, scores)

    # We expect one match
    assert len(result) == 1, "Expected exactly one match for single relation test."
    match = result[0]
    assert match.paragraph_id.full() == paragraph_id_str
    assert match.paragraph_id.paragraph_start == 10
    assert match.paragraph_id.paragraph_end == 20
    assert match.score == 0.8
    assert entity_name in match.relations.entities
    assert len(match.relations.entities[entity_name].related_to) == 1

    # Non-overlapping paragraphs
    entity_name_2 = "entityA"
    paragraph_id_str_1 = "blabla/u/link/10-20"
    paragraph_id_str_2 = "blabla/u/link/30-40"

    rel1 = DirectionalRelation(
        entity="entityB",
        entity_type=EntityType.ENTITY,
        entity_subtype="t",
        relation_label="bla",
        relation=RelationType.ENTITY,
        direction=RelationDirection.OUT,
        metadata=RelationMetadata(paragraph_id=paragraph_id_str_1),
        resource_id="blabla",
    )
    rel2 = DirectionalRelation(
        entity="entityC",
        entity_type=EntityType.ENTITY,
        entity_subtype="x",
        relation_label="bla2",
        relation=RelationType.ENTITY,
        direction=RelationDirection.OUT,
        metadata=RelationMetadata(paragraph_id=paragraph_id_str_2),
        resource_id="blabla",
    )

    relations_2 = Relations(entities={entity_name_2: EntitySubgraph(related_to=[rel1, rel2])})
    scores_2 = {entity_name_2: [0.5, 0.9]}

    result_2 = get_paragraph_info_from_relations(relations_2, scores_2)

    # We expect two distinct matches
    assert len(result_2) == 2, "Expected two matches for two distinct paragraphs test."
    # First paragraph
    match_1 = result_2[0]
    assert match_1.paragraph_id.full() == paragraph_id_str_1
    assert match_1.paragraph_id.paragraph_start == 10
    assert match_1.paragraph_id.paragraph_end == 20
    assert match_1.score == 0.5

    # Second paragraph
    match_2 = result_2[1]
    assert match_2.paragraph_id.full() == paragraph_id_str_2
    assert match_2.paragraph_id.paragraph_start == 30
    assert match_2.paragraph_id.paragraph_end == 40
    assert match_2.score == 0.9

    # Overlapping paragraphs
    entity_name_3 = "entityA"
    paragraph_id_str_contained = "blabla/u/link/10-20"
    paragraph_id_str_container = "blabla/u/link/10-30"

    rel1_contained = DirectionalRelation(
        entity="entityB",
        entity_type=EntityType.ENTITY,
        entity_subtype="t",
        relation_label="bla-contained",
        relation=RelationType.ENTITY,
        direction=RelationDirection.OUT,
        metadata=RelationMetadata(paragraph_id=paragraph_id_str_contained),
        resource_id="blabla",
    )
    rel2_container = DirectionalRelation(
        entity="entityC",
        entity_type=EntityType.ENTITY,
        entity_subtype="x",
        relation_label="bla-container",
        relation=RelationType.ENTITY,
        direction=RelationDirection.OUT,
        metadata=RelationMetadata(paragraph_id=paragraph_id_str_container),
        resource_id="blabla",
    )

    relations_3 = Relations(
        entities={entity_name_3: EntitySubgraph(related_to=[rel1_contained, rel2_container])}
    )
    scores_3 = {entity_name_3: [0.5, 0.9]}

    result_3 = get_paragraph_info_from_relations(relations_3, scores_3)

    assert len(result_3) == 1
    merged_paragraph = result_3[0]
    assert merged_paragraph.score == 0.9

    assert merged_paragraph.paragraph_id.full() == paragraph_id_str_container
    assert merged_paragraph.paragraph_id.paragraph_start == 10
    assert merged_paragraph.paragraph_id.paragraph_end == 30

    assert len(merged_paragraph.relations.entities[entity_name_3].related_to) == 2
