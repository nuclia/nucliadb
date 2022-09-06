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
import uuid

from httpx import AsyncClient

import pytest

from nucliadb_protos.noderesources_pb2 import Resource
from nucliadb_protos.noderesources_pb2 import ResourceID
from nucliadb_protos.utils_pb2 import Relation
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.nodereader_pb2 import RelationSearchRequest
from nucliadb_protos.nodereader_pb2 import RelationFilter
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_protos.writer_pb2 import BrokerMessage


@pytest.mark.asyncio
async def test_relations(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    """
    Test description:
    - Create a resource to assign some relations to it.
    - Using processing API, send a BrokerMessage with some relations
      for the resource
    - Validate the relations have been saved and are searchable
    """

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    e0 = RelationNode(value="E0", ntype=RelationNode.NodeType.ENTITY, subtype="")
    e1 = RelationNode(value="E1", ntype=RelationNode.NodeType.ENTITY, subtype="Official")
    e2 = RelationNode(value="E2", ntype=RelationNode.NodeType.ENTITY, subtype="Propaganda")
    r0 = Relation(relation=Relation.RelationType.CHILD, source=e0, to=e1, relation_label="R0")
    r1 = Relation(relation=Relation.RelationType.CHILD, source=e1, to=e2, relation_label="R1")
    r2 = Relation(relation=Relation.RelationType.CHILD, source=e2, to=e0, relation_label="R2")

    bm = BrokerMessage()
    bm.uuid = rid
    bm.kbid = knowledgebox
    bm.relations.extend([r0, r1, r2])

    async def iterate(value: BrokerMessage):
        yield value

    await nucliadb_grpc.ProcessMessage(iterate(bm))

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}?show=relations"
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["relations"]) == 3
