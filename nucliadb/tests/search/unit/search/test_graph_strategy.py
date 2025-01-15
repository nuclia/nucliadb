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

from nucliadb.search.search.graph_strategy import rank_relations
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
async def test_rank_relations(
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
                    ),
                    DirectionalRelation(
                        entity="Titanium Grade 3 Roofing Nails",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PRODUCT",
                        relation_label="has product",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                    ),
                    DirectionalRelation(
                        entity="New York",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="LOCATION",
                        relation_label="is located in",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
                    ),
                    DirectionalRelation(
                        entity="New York",
                        entity_type=EntityType.ENTITY,
                        entity_subtype="PLACE",
                        relation_label="is located in",
                        relation=RelationType.ENTITY,
                        direction=RelationDirection.OUT,
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
                    )
                ]
            ),
        }
    )
    result, scores = await rank_relations(
        relations=relations, query="my_query", kbid="my_kbid", user="my_user", top_k=3, score_threshold=0
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
