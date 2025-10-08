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
from uuid import uuid4

import pytest

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.train.resource import iterate_paragraphs
from nucliadb_protos.resources_pb2 import Basic as PBBasic
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.resources_pb2 import Metadata as PBMetadata
from nucliadb_protos.train_pb2 import EnabledMetadata, GetParagraphsRequest
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    Classification,
    FieldComputedMetadataWrapper,
    FieldID,
    Paragraph,
)


@pytest.mark.deploy_modes("component")
async def test_list_paragraphs(
    nucliadb_train_grpc: TrainStub, knowledgebox: str, test_pagination_resources
) -> None:
    req = GetParagraphsRequest()
    req.kb.uuid = knowledgebox
    req.metadata.entities = True
    req.metadata.labels = True
    req.metadata.text = True
    req.metadata.vector = True
    count = 0
    async for _ in nucliadb_train_grpc.GetParagraphs(req):  # type: ignore
        count += 1

    assert count == 30


@pytest.mark.deploy_modes("component")
async def test_list_paragraphs_shows_ners_with_positions(
    nucliadb_train_grpc: TrainStub, knowledgebox: str, test_pagination_resources
) -> None:
    req = GetParagraphsRequest()
    req.kb.uuid = knowledgebox
    req.metadata.entities = True
    req.metadata.labels = True
    req.metadata.text = True
    req.metadata.vector = True

    found_barcelona = found_manresa = False
    async for paragraph in nucliadb_train_grpc.GetParagraphs(req):  # type: ignore
        if "Barcelona" in paragraph.metadata.text:
            found_barcelona = True
            assert paragraph.metadata.entities == {"Barcelona": "CITY"}
            positions = paragraph.metadata.entity_positions["CITY/Barcelona"]
            assert positions.entity == "Barcelona"
            assert len(positions.positions) == 1
            assert positions.positions[0].start == 43
            assert positions.positions[0].end == 52
        elif "Manresa" in paragraph.metadata.text:
            found_manresa = True
            assert paragraph.metadata.entities == {"Manresa": "CITY"}
            positions = paragraph.metadata.entity_positions["CITY/Manresa"]
            assert positions.entity == "Manresa"
            assert len(positions.positions) == 2
            assert positions.positions[0].start == 22
            assert positions.positions[0].end == 29
            assert positions.positions[1].start == 38
            assert positions.positions[1].end == 45
    assert found_manresa and found_barcelona


async def test_iterate_paragraphs(storage, txn, cache, dummy_nidx_utility, knowledgebox: str):
    # Create a resource
    basic = PBBasic(
        icon="text/plain",
        title="My title",
        summary="My summary",
        thumbnail="/file",
    )
    basic.metadata.metadata["key"] = "value"
    basic.metadata.language = "ca"
    basic.metadata.useful = True
    basic.metadata.status = PBMetadata.Status.PROCESSED

    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug", basic=basic)
    assert r is not None

    # Add some labelled paragraphs to it
    bm = BrokerMessage()
    field1_if = FieldID()
    field1_if.field = "field1"
    field1_if.field_type = FieldType.TEXT
    fcmw = FieldComputedMetadataWrapper()
    fcmw.field.CopyFrom(field1_if)
    p1 = Paragraph()
    p1.start = 0
    p1.end = 82
    p1.classifications.append(Classification(labelset="ls1", label="label1"))
    p2 = Paragraph()
    p2.start = 84
    p2.end = 103
    p2.classifications.append(Classification(labelset="ls1", label="label2"))
    fcmw.metadata.metadata.paragraphs.append(p1)
    fcmw.metadata.metadata.paragraphs.append(p2)
    bm.field_metadata.append(fcmw)
    await r.apply_extracted(bm)

    # Check iterate paragraphs
    async for paragraph in iterate_paragraphs(r, EnabledMetadata(labels=True)):
        assert len(paragraph.metadata.labels.paragraph) == 1
        assert paragraph.metadata.labels.paragraph[0].label in ("label1", "label2")
