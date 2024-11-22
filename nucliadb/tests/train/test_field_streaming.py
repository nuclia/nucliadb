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

from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos.dataset_pb2 import (
    FieldStreamingBatch,
    TaskType,
    TrainSet,
)
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.train.utils import get_batches_from_train_response_stream
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder, FieldBuilder
from tests.utils.dirty_index import wait_for_sync


@pytest.mark.asyncio
async def test_generator_field_streaming(
    train_rest_api: aiohttp.ClientSession,
    nucliadb_grpc: WriterStub,
    knowledgebox: str,
):
    kbid = knowledgebox

    await inject_resources_with_paragraphs(kbid, nucliadb_grpc)

    async with train_rest_api.get(f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset") as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.FIELD_STREAMING
    trainset.batch_size = 5

    async with train_rest_api.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        async for batch in get_batches_from_train_response_stream(response, FieldStreamingBatch):
            batches.append(batch)
            assert len(batch.data) == 3
        assert len(batches) == 1


async def inject_resources_with_paragraphs(kbid: str, nucliadb_grpc: WriterStub):
    await inject_message(nucliadb_grpc, smb_wonder_bm(kbid))
    await wait_for_sync()
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
        field_builder.with_extracted_paragraph_metadata(rpb.Paragraph(start=start, end=end))
        start = end
    bmb.add_field_builder(field_builder)

    bm = bmb.build()

    return bm
