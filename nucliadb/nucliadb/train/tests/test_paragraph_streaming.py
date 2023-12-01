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
from typing import AsyncIterator

import aiohttp
import pytest
from nucliadb_protos.dataset_pb2 import ParagraphStreamingBatch, TaskType, TrainSet
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import inject_message
from nucliadb.tests.utils.broker_messages import BrokerMessageBuilder, FieldBuilder
from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb.train.tests.utils import get_batches_from_train_response_stream
from nucliadb_protos import resources_pb2 as rpb


async def get_paragraph_streaming_batch_from_response(
    response: aiohttp.ClientResponse,
) -> AsyncIterator[ParagraphStreamingBatch]:
    while True:
        header = await response.content.read(4)
        if header == b"":
            break
        payload_size = int.from_bytes(header, byteorder="big", signed=False)
        payload = await response.content.read(payload_size)
        pcb = ParagraphStreamingBatch()
        pcb.ParseFromString(payload)
        assert pcb.data
        yield pcb


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ["STABLE", "EXPERIMENTAL"], indirect=True)
async def test_generator_paragraph_streaming(
    train_rest_api: aiohttp.ClientSession,
    nucliadb_grpc: WriterStub,
    knowledgebox: str,
):
    kbid = knowledgebox

    await inject_resources_with_paragraphs(kbid, nucliadb_grpc)

    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.PARAGRAPH_STREAMING
    trainset.batch_size = 5
    trainset.filter.labels.append("labelset_paragraphs")

    async with train_rest_api.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        async for batch in get_batches_from_train_response_stream(
            response, ParagraphStreamingBatch
        ):
            batches.append(batch)
            assert len(batch.data) == 5
        assert len(batches) == 1


async def inject_resources_with_paragraphs(kbid: str, nucliadb_grpc: WriterStub):
    await inject_message(nucliadb_grpc, smb_wonder_bm(kbid))
    await asyncio.sleep(0.1)


def smb_wonder_bm(kbid: str) -> BrokerMessage:
    bmb = BrokerMessageBuilder(kbid=kbid)
    bmb.with_title("Super Mario Bros. Wonder")
    bmb.with_summary("SMB Wonder: the new Mario game from Nintendo")

    field_builder = FieldBuilder("smb-wonder", rpb.FieldType.FILE)
    paragraphs = [
        "Super Mario Bros. Wonder (SMB Wonder) is a 2023 platform game developed and published by Nintendo.\n",  # noqa
        "SMB Wonder is a side-scrolling plaftorm game.\n",
        "As one of eight player characters, the player completes levels across the Flower Kingdom.",  # noqa
    ]
    field_builder.with_extracted_text("".join(paragraphs))
    start = 0
    for paragraph in paragraphs:
        end = start + len(paragraph)
        field_builder.with_extracted_paragraph_metadata(
            rpb.Paragraph(start=start, end=end)
        )
        start = end
    bmb.add_field_builder(field_builder)

    bm = bmb.build()

    return bm
