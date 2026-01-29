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
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync

from nucliadb.common import datamanagers
from nucliadb.search.utilities import get_predict
from nucliadb_models.internal.predict import QueryInfo, SentenceSearch
from nucliadb_protos.resources_pb2 import (
    FieldType,
)
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage

PAD = [0.0] * 508
VECTORS = {
    "cat": [0.8, 0.6, 0.0, 0.0, *PAD],
    "mouse": [0.5, 0.8, 0.33, 0.0, *PAD],
    "dog": [0.0, 0.2, 0.9, 0.4, *PAD],
    "bigger": [0.0, 0.0, 0.0, 1.0, *PAD],
    "faster": [0.0, 0.0, 0.6, 0.8, *PAD],
    "lives": [1.0, 0.0, 0.0, 0.0, *PAD],
    "world": [0.0, 0.0, 1.0, 0.0, *PAD],
    "home": [0.2, 0.9, 0.4, 0.0, *PAD],
}


@pytest.fixture
async def graph_resource(nucliadb_writer: AsyncClient, nucliadb_ingest_grpc, standalone_knowledgebox):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "Knowledge graph",
            "slug": "knowledgegraph",
            "summary": "Test knowledge graph",
            "texts": {
                "animals": {"body": "Cats are bigger than mice but not as big as dogs"},
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    vectorset = None
    async with datamanagers.with_ro_transaction() as txn:
        async for vectorset_id, vs in datamanagers.vectorsets.iter(txn, kbid=standalone_knowledgebox):
            vectorset = vectorset_id
            break
    assert vectorset is not None

    nodes = {
        "cat": RelationNode(value="Cat", ntype=RelationNode.NodeType.ENTITY, subtype="ANIMAL"),
        "dog": RelationNode(value="Dog", ntype=RelationNode.NodeType.ENTITY, subtype="ANIMAL"),
        "mouse": RelationNode(value="Mouse", ntype=RelationNode.NodeType.ENTITY, subtype="ANIMAL"),
        "world": RelationNode(value="Worldwide", ntype=RelationNode.NodeType.ENTITY, subtype="PLACE"),
        "home": RelationNode(value="Home", ntype=RelationNode.NodeType.ENTITY, subtype="PLACE"),
    }

    bmb = BrokerMessageBuilder(
        kbid=standalone_knowledgebox,
        rid=rid,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    field = bmb.field_builder("animals", FieldType.TEXT)
    field.with_extracted_text("Cats are bigger than mice but not as big as dogs")
    field.add_relation(
        nodes["cat"],
        "bigger_than",
        nodes["mouse"],
        {vectorset: VECTORS["cat"]},
        {vectorset: VECTORS["bigger"]},
        {vectorset: VECTORS["mouse"]},
    )
    field.add_relation(
        nodes["dog"],
        "bigger_than",
        nodes["cat"],
        {vectorset: VECTORS["dog"]},
        {vectorset: VECTORS["bigger"]},
        {vectorset: VECTORS["cat"]},
    )
    field.add_relation(
        nodes["cat"],
        "faster_than",
        nodes["dog"],
        {vectorset: VECTORS["cat"]},
        {vectorset: VECTORS["faster"]},
        {vectorset: VECTORS["dog"]},
    )
    field.add_relation(
        nodes["dog"],
        "faster_than",
        nodes["mouse"],
        {vectorset: VECTORS["dog"]},
        {vectorset: VECTORS["faster"]},
        {vectorset: VECTORS["mouse"]},
    )
    field.add_relation(
        nodes["dog"],
        "lives",
        nodes["home"],
        {vectorset: VECTORS["dog"]},
        {vectorset: VECTORS["lives"]},
        {vectorset: VECTORS["home"]},
    )
    field.add_relation(
        nodes["cat"],
        "lives",
        nodes["home"],
        {vectorset: VECTORS["cat"]},
        {vectorset: VECTORS["lives"]},
        {vectorset: VECTORS["home"]},
    )
    field.add_relation(
        nodes["mouse"],
        "lives",
        nodes["world"],
        {vectorset: VECTORS["mouse"]},
        {vectorset: VECTORS["lives"]},
        {vectorset: VECTORS["world"]},
    )
    bm = bmb.build()

    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()
    yield rid


@pytest.mark.deploy_modes("standalone")
async def test_serialize(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource: str,
):
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{graph_resource}",
        params=dict(
            show=["relations", "extracted"],
            extracted=["metadata", "relation_vectors"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    extracted = body["data"]["texts"]["animals"]["extracted"]
    assert len(extracted["relation_node_vectors"]["multilingual"]) == 5
    assert len(extracted["relation_edge_vectors"]["multilingual"]) == 3


@pytest.mark.deploy_modes("standalone")
async def test_node_queries(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource: str,
):
    predict = get_predict()
    with patch.object(
        predict,
        "query",
        AsyncMock(
            return_value=QueryInfo(
                language=None,
                visual_llm=False,
                query="cat",
                max_context=10,
                entities=None,
                sentence=SentenceSearch(vectors={"multilingual": VECTORS["cat"]}),
            )
        ),
    ):
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/graph/nodes",
            json={"query": {"prop": "node", "value": "dog", "match": "semantic"}},
        )

    assert resp.status_code == 200
    body = resp.json()
    nodes = body["nodes"]

    assert len(nodes) == 2
    assert nodes[0]["value"] == "Cat"
    assert nodes[1]["value"] == "Mouse"
    assert nodes[0]["score"] > nodes[1]["score"]


@pytest.mark.deploy_modes("standalone")
async def test_relation_queries(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource: str,
):
    predict = get_predict()
    with patch.object(
        predict,
        "query",
        AsyncMock(
            return_value=QueryInfo(
                language=None,
                visual_llm=False,
                query="faster",
                max_context=10,
                entities=None,
                sentence=SentenceSearch(vectors={"multilingual": VECTORS["faster"]}),
            )
        ),
    ):
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/graph/relations",
            json={"query": {"prop": "relation", "label": "faster", "match": "semantic"}},
        )

    assert resp.status_code == 200
    body = resp.json()
    relations = body["relations"]

    assert len(relations) == 2
    assert relations[0]["label"] == "faster_than"
    assert relations[1]["label"] == "bigger_than"
    assert relations[0]["score"] > relations[1]["score"]


@pytest.mark.deploy_modes("standalone")
async def test_path_queries(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource: str,
):
    predict = get_predict()
    with patch.object(
        predict,
        "query",
        AsyncMock(
            return_value=QueryInfo(
                language=None,
                visual_llm=False,
                query="dog",
                max_context=10,
                entities=None,
                sentence=SentenceSearch(vectors={"multilingual": VECTORS["dog"]}),
            )
        ),
    ):
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/graph",
            json={"query": {"prop": "node", "value": "dog", "match": "semantic"}},
        )

    assert resp.status_code == 200
    body = resp.json()
    paths = body["paths"]

    assert len(paths) == 5

    def to_text(path):
        return f"{path['source']['value']} {path['relation']['label']} {path['destination']['value']}"

    assert sorted([to_text(p) for p in paths]) == sorted(
        [
            "Cat faster_than Dog",
            "Mouse lives Worldwide",  # Dog & Worldwide vectors are close
            "Dog bigger_than Cat",
            "Dog faster_than Mouse",
            "Dog lives Home",
        ]
    )
    assert (
        paths[0]["score"]
        >= paths[1]["score"]
        >= paths[2]["score"]
        >= paths[3]["score"]
        >= paths[4]["score"]
    )

    # Repeat, excluding Worldwide via the node subtype filter
    with patch.object(
        predict,
        "query",
        AsyncMock(
            return_value=QueryInfo(
                language=None,
                visual_llm=False,
                query="dog",
                max_context=10,
                entities=None,
                sentence=SentenceSearch(vectors={"multilingual": VECTORS["dog"]}),
            )
        ),
    ):
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/graph",
            json={"query": {"prop": "node", "value": "dog", "match": "semantic", "group": "ANIMAL"}},
        )

    assert resp.status_code == 200
    body = resp.json()
    paths = body["paths"]

    assert len(paths) == 4

    assert sorted([to_text(p) for p in paths]) == sorted(
        [
            "Cat faster_than Dog",
            "Dog bigger_than Cat",
            "Dog faster_than Mouse",
            "Dog lives Home",
        ]
    )
    assert paths[0]["score"] >= paths[1]["score"] >= paths[2]["score"] >= paths[3]["score"]

    # Repeat, excluding Worldwide via the node subtype filter and faster_than via exact match
    with patch.object(
        predict,
        "query",
        AsyncMock(
            return_value=QueryInfo(
                language=None,
                visual_llm=False,
                query="dog",
                max_context=10,
                entities=None,
                sentence=SentenceSearch(vectors={"multilingual": VECTORS["dog"]}),
            )
        ),
    ):
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/graph",
            json={
                "query": {
                    "and": [
                        {"prop": "node", "value": "dog", "match": "semantic", "group": "ANIMAL"},
                        {"not": {"prop": "relation", "label": "faster_than"}},
                    ]
                }
            },
        )

    assert resp.status_code == 200
    body = resp.json()
    paths = body["paths"]

    assert len(paths) == 2

    assert sorted([to_text(p) for p in paths]) == sorted(
        [
            "Dog bigger_than Cat",
            "Dog lives Home",
        ]
    )
    assert paths[0]["score"] >= paths[1]["score"]
