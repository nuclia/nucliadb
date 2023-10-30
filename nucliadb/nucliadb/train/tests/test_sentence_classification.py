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
import uuid
from datetime import datetime
from typing import AsyncIterator

import aiohttp
import pytest
from nucliadb_protos.dataset_pb2 import SentenceClassificationBatch, TaskType, TrainSet
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import inject_message
from nucliadb.tests.utils.broker_messages import BrokerMessageBuilder, FieldBuilder
from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb_protos import resources_pb2 as rpb


async def get_sentence_classification_batch_from_response(
    response: aiohttp.ClientResponse,
) -> AsyncIterator[SentenceClassificationBatch]:
    while True:
        header = await response.content.read(4)
        if header == b"":
            break
        payload_size = int.from_bytes(header, byteorder="big", signed=False)
        payload = await response.content.read(payload_size)
        pcb = SentenceClassificationBatch()
        pcb.ParseFromString(payload)
        assert pcb.data
        yield pcb


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ["STABLE", "EXPERIMENTAL"], indirect=True)
async def test_generator_sentence_classification(
    train_rest_api: aiohttp.ClientSession,
    nucliadb_grpc: WriterStub,
    knowledgebox_with_labels: str,
):
    kbid = knowledgebox_with_labels

    await inject_resource_with_sentence_classification(kbid, nucliadb_grpc)

    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.SENTENCE_CLASSIFICATION
    trainset.batch_size = 2
    trainset.filter.labels.append("labelset_paragraphs")

    async with train_rest_api.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        async for batch in get_sentence_classification_batch_from_response(response):
            batches.append(batch)
            assert len(batch.data) == 2
        assert len(batches) == 2


async def inject_resource_with_sentence_classification(knowledgebox, writer):
    bm = broker_resource(knowledgebox)
    await inject_message(writer, bm)
    await asyncio.sleep(0.1)
    return bm.uuid


def broker_resource(knowledgebox: str) -> BrokerMessage:
    rid = str(uuid.uuid4())
    bmb = BrokerMessageBuilder(kbid=knowledgebox, rid=rid)
    bmb.with_title("Title Resource")
    bmb.with_summary("Summary of document")
    bmb.with_resource_labels("labelset_resources", ["label_user"])

    file_field = FieldBuilder("file", rpb.FieldType.FILE)
    file_field.with_extracted_text(
        "My own text Ramon. This is great to be here. \n Where is my beer? Do you want to go shooping? This is a test!"  # noqa
    )

    labelset = "labelset_paragraphs"
    labels = ["label_user"]
    file_field.with_user_paragraph_labels(f"{rid}/f/file/0-45", labelset, labels)
    file_field.with_user_paragraph_labels(f"{rid}/f/file/47-64", labelset, labels)
    file_field.with_user_paragraph_labels(f"{rid}/f/file/65-93", labelset, labels)
    file_field.with_user_paragraph_labels(f"{rid}/f/file/93-109", labelset, labels)

    classification = rpb.Classification(
        labelset="labelset_paragraphs", label="label_machine"
    )
    file_field.with_extracted_paragraph_metadata(
        rpb.Paragraph(
            start=0,
            end=45,
            classifications=[classification],
            sentences=[rpb.Sentence(start=0, end=45)],
        )
    )
    file_field.with_extracted_paragraph_metadata(
        rpb.Paragraph(
            start=47,
            end=64,
            classifications=[classification],
            sentences=[rpb.Sentence(start=47, end=64)],
        )
    )
    file_field.with_extracted_paragraph_metadata(
        rpb.Paragraph(
            start=65,
            end=93,
            classifications=[classification],
            sentences=[rpb.Sentence(start=65, end=93)],
        )
    )
    file_field.with_extracted_paragraph_metadata(
        rpb.Paragraph(
            start=93,
            end=109,
            classifications=[classification],
            sentences=[rpb.Sentence(start=94, end=109)],
        )
    )

    file_field.with_extracted_labels("labelset_resources", ["label_machine"])

    bmb.add_field_builder(file_field)

    bm = bmb.build()

    bm.files["file"].added.FromDatetime(datetime.now())
    bm.files["file"].file.source = rpb.CloudFile.Source.EXTERNAL

    return bm
