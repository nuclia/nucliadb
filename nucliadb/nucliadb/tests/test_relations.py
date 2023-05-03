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
from typing import Tuple

import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import (
    FieldComputedMetadataWrapper,
    FieldType,
    Relations,
)
from nucliadb_protos.utils_pb2 import Relation, RelationMetadata, RelationNode
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import inject_message


@pytest.fixture
@pytest.mark.asyncio
async def resource_with_bm_relations(
    nucliadb_grpc: WriterStub,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "texts": {"text1": {"body": "Mickey loves Minnie"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    bm = await create_broker_message_with_relations()
    bm.kbid = knowledgebox
    bm.uuid = rid

    await inject_message(nucliadb_grpc, bm)

    yield rid, "text1"


@pytest.mark.asyncio
async def test_api_aliases(
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
    resource_with_bm_relations: Tuple[str, str],
):
    rid, field_id = resource_with_bm_relations

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}",
        params=dict(
            show=["relations", "extracted"],
            extracted=["metadata"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    extracted_metadata = body["data"]["texts"]["text1"]["extracted"]["metadata"]
    assert len(extracted_metadata["metadata"]["relations"]) == 1
    assert "from" in extracted_metadata["metadata"]["relations"][0]
    assert "from_" not in extracted_metadata["metadata"]["relations"][0]

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}/text/{field_id}",
        params=dict(
            show=["extracted"],
            extracted=["metadata"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["extracted"]["metadata"]["metadata"]["relations"]) == 1
    assert "from" in body["extracted"]["metadata"]["metadata"]["relations"][0]
    assert "from_" not in body["extracted"]["metadata"]["metadata"]["relations"][0]


@pytest.mark.asyncio
async def test_broker_message_relations(
    nucliadb_reader: AsyncClient,
    knowledgebox: str,
    resource_with_bm_relations: Tuple[str, str],
):
    """
    Test description:
    - Create a resource to assign some relations to it.
    - Using processing API, send a BrokerMessage with some relations
      for the resource
    - Validate the relations have been saved and are searchable
    """
    rid, field_id = resource_with_bm_relations

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}",
        params=dict(
            show=["relations", "extracted"],
            extracted=["metadata"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    # Resource level relations
    assert len(body["relations"]) == 3

    # Field level relations
    extracted_metadata = body["data"]["texts"]["text1"]["extracted"]["metadata"]
    assert len(extracted_metadata["metadata"]["relations"]) == 1
    relation = extracted_metadata["metadata"]["relations"][0]
    assert relation["label"] == "love"
    assert relation["metadata"] == dict(
        paragraph_id="foo", source_start=1, source_end=2, to_start=10, to_end=11
    )

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}/text/{field_id}",
        params=dict(
            show=["extracted"],
            extracted=["metadata"],
        ),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["extracted"]["metadata"]["metadata"]["relations"]) == 1


@pytest.mark.asyncio
async def test_extracted_relations(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    """
    Test description:

    Create a resource with fields from which relations can be
    extracted and test it.
    """
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "My resource",
            "slug": "myresource",
            "summary": "Some summary",
            "origin": {
                "collaborators": ["Anne", "John"],
            },
            "usermetadata": {
                "classifications": [
                    {"labelset": "labelset-1", "label": "label-1"},
                    {"labelset": "labelset-2", "label": "label-2"},
                ],
                "relations": [
                    {
                        "relation": "CHILD",
                        "to": {"type": "resource", "value": "document-uuid"},
                    },
                    {"relation": "ABOUT", "to": {"type": "label", "value": "label"}},
                    {
                        "relation": "ENTITY",
                        "to": {
                            "type": "entity",
                            "value": "entity-1",
                            "group": "entity-type-1",
                        },
                    },
                    {
                        "relation": "COLAB",
                        "to": {
                            "type": "user",
                            "value": "user",
                        },
                    },
                    {
                        "relation": "OTHER",
                        "to": {
                            "type": "label",
                            "value": "other",
                        },
                    },
                ],
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}?show=basic")
    assert resp.status_code == 200
    assert len(resp.json()["usermetadata"]["relations"]) == 5


async def create_broker_message_with_relations():
    e0 = RelationNode(value="E0", ntype=RelationNode.NodeType.ENTITY, subtype="")
    e1 = RelationNode(
        value="E1", ntype=RelationNode.NodeType.ENTITY, subtype="Official"
    )
    e2 = RelationNode(
        value="E2", ntype=RelationNode.NodeType.ENTITY, subtype="Propaganda"
    )
    r0 = Relation(
        relation=Relation.RelationType.CHILD, source=e0, to=e1, relation_label="R0"
    )
    r1 = Relation(
        relation=Relation.RelationType.CHILD, source=e1, to=e2, relation_label="R1"
    )
    r2 = Relation(
        relation=Relation.RelationType.CHILD, source=e2, to=e0, relation_label="R2"
    )
    mickey = RelationNode(
        value="Mickey", ntype=RelationNode.NodeType.ENTITY, subtype=""
    )
    minnie = RelationNode(
        value="Minnie", ntype=RelationNode.NodeType.ENTITY, subtype="Official"
    )
    love_relation = Relation(
        relation=Relation.RelationType.CHILD,
        source=mickey,
        to=minnie,
        relation_label="love",
        metadata=RelationMetadata(
            paragraph_id="foo",
            source_start=1,
            source_end=2,
            to_start=10,
            to_end=11,
        ),
    )
    bm = BrokerMessage()

    # Add relations at the resource level
    bm.relations.extend([r0, r1, r2])

    # Add relations at the field level
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.field_type = FieldType.TEXT
    fcmw.field.field = "text1"
    relations = Relations()
    relations.relations.extend([love_relation])
    fcmw.metadata.metadata.relations.append(relations)
    bm.field_metadata.append(fcmw)

    return bm
