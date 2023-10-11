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
from nucliadb_protos.dataset_pb2 import ParagraphClassificationBatch, TaskType, TrainSet
from nucliadb_protos.knowledgebox_pb2 import Label, LabelSet
from nucliadb_protos.writer_pb2 import SetLabelsRequest
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX


async def get_paragraph_classification_batch_from_response(
    response: aiohttp.ClientResponse,
) -> AsyncIterator[ParagraphClassificationBatch]:
    while True:
        header = await response.content.read(4)
        if header == b"":
            break
        payload_size = int.from_bytes(header, byteorder="big", signed=False)
        payload = await response.content.read(payload_size)
        pcb = ParagraphClassificationBatch()
        pcb.ParseFromString(payload)
        assert pcb.data
        yield pcb


@pytest.mark.asyncio
async def test_generator_paragraph_classification(
    train_rest_api: aiohttp.ClientSession,
    knowledgebox: str,
    resource_with_paragraph_labels: str,
    nucliadb_grpc: WriterStub,
):
    slr = SetLabelsRequest()
    slr.kb.uuid = knowledgebox
    slr.id = "labelset_paragraphs"
    slr.labelset.kind.append(LabelSet.LabelSetKind.PARAGRAPHS)
    l1 = Label(title="label_machine")
    l2 = Label(title="label_user")
    slr.labelset.labels.append(l1)
    slr.labelset.labels.append(l2)
    await nucliadb_grpc.SetLabels(slr)  # type: ignore

    slr = SetLabelsRequest()
    slr.kb.uuid = knowledgebox
    slr.id = "labelset_resources"
    slr.labelset.kind.append(LabelSet.LabelSetKind.RESOURCES)
    l1 = Label(title="label_machine")
    l2 = Label(title="label_user")
    slr.labelset.labels.append(l1)
    slr.labelset.labels.append(l2)
    await nucliadb_grpc.SetLabels(slr)  # type: ignore

    await asyncio.sleep(0.1)
    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{knowledgebox}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.PARAGRAPH_CLASSIFICATION
    trainset.batch_size = 2
    trainset.filter.labels.append("labelset_paragraphs")

    async with train_rest_api.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{knowledgebox}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        async for batch in get_paragraph_classification_batch_from_response(response):
            batches.append(batch)
            assert len(batch.data) == 2
        assert len(batches) == 2
