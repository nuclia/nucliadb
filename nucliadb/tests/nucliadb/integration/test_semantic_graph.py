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


@pytest.fixture(scope="function")
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
    }
    vectors = {"cat": [0.0] * 512, "dog": [1.0] * 512, "bigger": [0.5] * 512, "mouse": [0.2] * 512}

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
        {vectorset: vectors["cat"]},
        {vectorset: vectors["bigger"]},
        {vectorset: vectors["mouse"]},
    )
    field.add_relation(
        nodes["dog"],
        "bigger_than",
        nodes["cat"],
        {vectorset: vectors["dog"]},
        {vectorset: vectors["bigger"]},
        {vectorset: vectors["cat"]},
    )
    bm = bmb.build()

    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()
    yield rid


@pytest.mark.deploy_modes("standalone")
async def test_ingestion(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    graph_resource: str,
):
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{graph_resource}",
        params=dict(
            show=["relations", "extracted"],
            extracted=["metadata"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()

    # TODO: Check that get returns relation vectors?

    # Check that it is indexed and we can search for it
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
                sentence=SentenceSearch(vectors={"multilingual": [0.3] * 512}),
            )
        ),
    ):
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/graph/nodes",
            json={"query": {"prop": "node", "value": "dog", "match": "semantic"}},
        )

    assert resp.status_code == 200, resp.text
    body = resp.json()
    # TODO: Check scores once we add them to the response, use better fake vectors
    assert sorted([n["value"] for n in body["nodes"]]) == ["Dog", "Mouse"]
