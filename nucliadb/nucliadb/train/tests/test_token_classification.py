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

import aiohttp
import pytest
from nucliadb_protos.dataset_pb2 import TaskType, TokenClassificationBatch, TrainSet
from nucliadb_protos.resources_pb2 import Position
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import inject_message
from nucliadb.tests.utils.broker_messages import BrokerMessageBuilder, FieldBuilder
from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb.train.tests.utils import get_batches_from_train_response_stream
from nucliadb_protos import resources_pb2 as rpb


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ["STABLE", "EXPERIMENTAL"], indirect=True)
async def test_generator_token_classification(
    train_rest_api: aiohttp.ClientSession,
    knowledgebox_with_entities: str,
    nucliadb_grpc: WriterStub,
):
    kbid = knowledgebox_with_entities

    await inject_resource_with_token_classification(kbid, nucliadb_grpc)

    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset"
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
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches: list[TokenClassificationBatch] = []
        async for batch in get_batches_from_train_response_stream(
            response, TokenClassificationBatch
        ):
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


async def inject_resource_with_token_classification(knowledgebox, writer):
    bm = broker_resource(knowledgebox)
    await inject_message(writer, bm)
    await asyncio.sleep(0.1)
    return bm.uuid


def broker_resource(knowledgebox: str) -> BrokerMessage:
    bmb = BrokerMessageBuilder(kbid=knowledgebox)

    bmb.with_title("This is a bird, its a plane, no, its el Super Fran")
    title_field = bmb.field_builder("title", rpb.FieldType.GENERIC)
    title_field.with_extracted_entity(
        "PERSON", "el Super Fran", positions=[Position(start=37, end=50)]
    )

    bmb.with_summary("Summary of Nuclia using Debian")
    summary_field = bmb.field_builder("summary", rpb.FieldType.GENERIC)
    summary_field.with_extracted_entity(
        "ORG", "Nuclia", positions=[Position(start=11, end=17)]
    )
    summary_field.with_extracted_entity(
        "ORG", "Debian", positions=[Position(start=24, end=30)]
    )

    file_field = FieldBuilder("file", rpb.FieldType.FILE)
    file_field.with_extracted_text(
        "My own text Ramon. This is great to be at Nuclia. \n Where is the Generalitat de Catalunya? Eudald Camprubi, do you want to go shooping? This is a test Carmen Iniesta!"  # noqa
    )
    file_field.with_extracted_paragraph_metadata(rpb.Paragraph(start=0, end=49))
    file_field.with_extracted_paragraph_metadata(rpb.Paragraph(start=50, end=90))
    file_field.with_extracted_paragraph_metadata(rpb.Paragraph(start=91, end=135))
    file_field.with_extracted_paragraph_metadata(rpb.Paragraph(start=136, end=166))

    file_field.with_user_entity("PERSON", "Ramon", start=12, end=17)
    file_field.with_user_entity("ORG", "Nuclia", start=42, end=48)
    file_field.with_user_entity("ORG", "Generalitat de Catalunya", start=65, end=89)
    file_field.with_user_entity("PERSON", "Eudald", start=91, end=106)
    file_field.with_user_entity("PERSON", "Carmen Iniesta", start=151, end=165)

    bmb.add_field_builder(file_field)

    bm = bmb.build()

    return bm
