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

import asyncio
from collections import OrderedDict
from typing import AsyncIterator, List

import aiohttp
import pytest
from nucliadb_protos.dataset_pb2 import TaskType, TokenClassificationBatch, TrainSet
from nucliadb_protos.resources_pb2 import (
    Metadata,
    Position,
    TokenSplit,
    UserFieldMetadata,
)
from nucliadb_protos.writer_pb2 import BrokerMessage, SetEntitiesRequest
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import inject_message
from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb.train.generators.token_classifier import process_entities


async def get_token_classification_batch_from_response(
    response: aiohttp.ClientResponse,
) -> AsyncIterator[TokenClassificationBatch]:
    while True:
        header = await response.content.read(4)
        if header in [b"", None]:
            break
        payload_size = int.from_bytes(header, byteorder="big", signed=False)
        payload = await response.content.read(payload_size)
        pcb = TokenClassificationBatch()
        pcb.ParseFromString(payload)
        assert pcb.data
        yield pcb


def broker_resource(knowledgebox: str) -> BrokerMessage:
    import uuid
    from datetime import datetime

    from nucliadb_protos import resources_pb2 as rpb

    rid = str(uuid.uuid4())
    slug = f"{rid}slug1"

    bm: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug=slug,
        type=BrokerMessage.AUTOCOMMIT,
    )

    bm.basic.icon = "text/plain"
    bm.basic.title = "This is a bird, its a plane, no, its el Super Fran"
    bm.basic.summary = "Summary of Nuclia using Debian"
    bm.basic.thumbnail = "doc"
    bm.basic.layout = "default"
    bm.basic.metadata.useful = True
    bm.basic.metadata.status = Metadata.Status.PROCESSED
    bm.basic.metadata.language = "es"
    bm.basic.created.FromDatetime(datetime.now())
    bm.basic.modified.FromDatetime(datetime.now())
    bm.origin.source = rpb.Origin.Source.WEB

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "My own text Ramon. This is great to be at Nuclia. \n Where is the Generalitat de Catalunya? Eudald Camprubi, do you want to go shooping? This is a test Carmen Iniesta!"  # noqa
    etw.field.field = "file"
    etw.field.field_type = rpb.FieldType.FILE
    bm.extracted_text.append(etw)

    ufm = UserFieldMetadata()

    ts = TokenSplit()
    ts.token = "Ramon"
    ts.klass = "PERSON"
    ts.start = 12
    ts.end = 17
    ufm.token.append(ts)

    ts = TokenSplit()
    ts.token = "Nuclia"
    ts.klass = "ORG"
    ts.start = 42
    ts.end = 48
    ufm.token.append(ts)

    ts = TokenSplit()
    ts.token = "Generalitat de Catalunya"
    ts.klass = "ORG"
    ts.start = 65
    ts.end = 89
    ufm.token.append(ts)

    ts = TokenSplit()
    ts.token = "Eudald Camprubi"
    ts.klass = "PERSON"
    ts.start = 91
    ts.end = 106
    ufm.token.append(ts)

    ts = TokenSplit()
    ts.token = "Carmen Iniesta"
    ts.klass = "PERSON"
    ts.start = 151
    ts.end = 165
    ufm.token.append(ts)

    ufm.field.field = "file"
    ufm.field.field_type = rpb.FieldType.FILE
    bm.basic.fieldmetadata.append(ufm)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Summary of Nuclia using Debian"
    etw.field.field = "summary"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "This is a bird, its a plane, no, its el Super Fran"
    etw.field.field = "title"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    bm.files["file"].added.FromDatetime(datetime.now())
    bm.files["file"].file.source = rpb.CloudFile.Source.EXTERNAL

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "file"
    fcm.field.field_type = rpb.FieldType.FILE
    p1 = rpb.Paragraph(
        start=0,
        end=49,
    )
    p2 = rpb.Paragraph(
        start=50,
        end=90,
    )

    p3 = rpb.Paragraph(
        start=91,
        end=135,
    )

    p4 = rpb.Paragraph(
        start=136,
        end=166,
    )

    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.paragraphs.append(p2)
    fcm.metadata.metadata.paragraphs.append(p3)
    fcm.metadata.metadata.paragraphs.append(p4)
    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())

    bm.field_metadata.append(fcm)

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "title"
    fcm.field.field_type = rpb.FieldType.GENERIC
    fcm.metadata.metadata.positions["PERSON/el Super Fran"].entity = "el Super Fran"
    pos = Position(start=37, end=50)
    fcm.metadata.metadata.positions["PERSON/el Super Fran"].position.append(pos)
    bm.field_metadata.append(fcm)

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "summary"
    fcm.field.field_type = rpb.FieldType.GENERIC
    fcm.metadata.metadata.positions["ORG/Nuclia"].entity = "Nuclia"
    pos = Position(start=11, end=17)
    fcm.metadata.metadata.positions["ORG/Nuclia"].position.append(pos)
    fcm.metadata.metadata.positions["ORG/Debian"].entity = "Debian"
    pos = Position(start=24, end=30)
    fcm.metadata.metadata.positions["ORG/Debian"].position.append(pos)
    bm.field_metadata.append(fcm)

    bm.source = BrokerMessage.MessageSource.WRITER
    return bm


async def inject_resource_with_token_classification(knowledgebox, writer):
    bm = broker_resource(knowledgebox)
    await inject_message(writer, bm)
    return bm.uuid


@pytest.mark.asyncio
async def test_generator_token_classification(
    train_rest_api: aiohttp.ClientSession, knowledgebox: str, nucliadb_grpc: WriterStub
):
    # Create Entities
    ser = SetEntitiesRequest()
    ser.kb.uuid = knowledgebox
    ser.group = "PERSON"
    ser.entities.title = "PERSON"
    ser.entities.entities["Ramon"].value = "Ramon"
    ser.entities.entities["Eudald Camprubi"].value = "Eudald Camprubi"
    ser.entities.entities["Carmen Iniesta"].value = "Carmen Iniesta"
    ser.entities.entities["el Super Fran"].value = "el Super Fran"
    await nucliadb_grpc.SetEntities(ser)  # type: ignore

    ser = SetEntitiesRequest()
    ser.kb.uuid = knowledgebox
    ser.group = "ORG"
    ser.entities.title = "ORG"
    ser.entities.entities["Nuclia"].value = "Nuclia"
    ser.entities.entities["Debian"].value = "Debian"
    ser.entities.entities["Generalitat de Catalunya"].value = "Generalitat de Catalunya"
    await nucliadb_grpc.SetEntities(ser)  # type: ignore

    await inject_resource_with_token_classification(knowledgebox, nucliadb_grpc)
    await asyncio.sleep(0.1)
    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{knowledgebox}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.TOKEN_CLASSIFICATION
    trainset.batch_size = 2
    trainset.filter.labels.append("PERSON")
    trainset.filter.labels.append("ORG")
    async with train_rest_api.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{knowledgebox}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches: List[TokenClassificationBatch] = []
        async for batch in get_token_classification_batch_from_response(response):
            batches.append(batch)

    for batch in batches:
        if batch.data[0].token == "Eudald":
            assert batch.data[0].label == "B-PERSON"
            assert batch.data[1].label == "I-PERSON"
            assert batch.data[2].label == "O"
        if batch.data[0].token == "This":
            assert batch.data[4].label == "B-PERSON"
            assert batch.data[5].label == "I-PERSON"
        if batch.data[0].token == "Where":
            assert batch.data[3].label == "B-ORG"
            assert batch.data[4].label == "I-ORG"
            assert batch.data[5].label == "I-ORG"
        if batch.data[0].token == "Summary":
            assert batch.data[2].label == "B-ORG"
            assert batch.data[4].label == "B-ORG"
        if batch.data[0].token == "My":
            assert batch.data[3].label == "B-PERSON"
            assert batch.data[12].label == "B-ORG"


def test_process_entities():
    split_text = {"__main__": "This is a bird, its a plane, no, its el Super Fran"}
    split_ners = {"__main__": OrderedDict([((37, 50), ("PERSON", "el Super Fran"))])}
    split_paragaphs = {"__main__": []}
    entities = list(
        process_entities(
            split_text["__main__"], split_ners["__main__"], split_paragaphs["__main__"]
        )
    )
    assert entities == [
        [
            ("This", "O"),
            ("is", "O"),
            ("a", "O"),
            ("bird,", "O"),
            ("its", "O"),
            ("a", "O"),
            ("plane,", "O"),
            ("no,", "O"),
            ("its", "O"),
            ("el", "B-PERSON"),
            ("Super", "I-PERSON"),
            ("Fran", "I-PERSON"),
        ]
    ]
