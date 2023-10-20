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
from nucliadb_protos.dataset_pb2 import FieldClassificationBatch, TaskType, TrainSet
from nucliadb_protos.knowledgebox_pb2 import Label, LabelSet
from nucliadb_protos.writer_pb2 import SetLabelsRequest
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import inject_message
from nucliadb.tests.utils.broker_messages import BrokerMessageBuilder
from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX


async def get_field_classification_batch_from_response(
    response: aiohttp.ClientResponse,
) -> AsyncIterator[FieldClassificationBatch]:
    while True:
        header = await response.content.read(4)
        if header == b"":
            break
        payload_size = int.from_bytes(header, byteorder="big", signed=False)
        payload = await response.content.read(payload_size)
        pcb = FieldClassificationBatch()
        pcb.ParseFromString(payload)
        assert pcb.data
        yield pcb


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ["STABLE", "EXPERIMENTAL"], indirect=True)
async def test_generator_field_classification(
    train_rest_api: aiohttp.ClientSession,
    knowledgebox_with_labels: str,
):
    kbid = knowledgebox_with_labels

    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.FIELD_CLASSIFICATION
    trainset.batch_size = 2

    tests = [
        # all fields
        ([], 2, 4),
        (["labelset_resources"], 2, 4),
        # 2 fields
        (["labelset_resources/label_user"], 1, 2),
        # unused label
        (["labelset_resources/label_alien"], 0, 0),
        # non existent
        (["nonexistent_labelset"], 0, 0),
    ]

    for labels, expected_batches, expected_total in tests:
        trainset.filter.ClearField("labels")
        trainset.filter.labels.extend(labels)  # type: ignore

        async with train_rest_api.post(
            f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
            data=trainset.SerializeToString(),
        ) as response:
            assert response.status == 200
            batches = []
            total = 0
            async for batch in get_field_classification_batch_from_response(response):
                batches.append(batch)
                total += len(batch.data)
            assert len(batches) == expected_batches
            assert total == expected_total


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ["STABLE", "EXPERIMENTAL"], indirect=True)
async def test_generator_field_classification_without_labels(
    train_rest_api: aiohttp.ClientSession,
    knowledgebox_without_labels: str,
):
    kbid = knowledgebox_without_labels

    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.FIELD_CLASSIFICATION
    trainset.batch_size = 2

    tests = [
        # all fields
        ([], 2, 4),
        (["labelset_resources"], 0, 0),
        # 2 fields
        (["labelset_resources/label_user"], 0, 0),
        # non existent
        (["nonexistent_labelset"], 0, 0),
    ]

    for labels, expected_batches, expected_total in tests:
        trainset.filter.ClearField("labels")
        trainset.filter.labels.extend(labels)  # type: ignore

        async with train_rest_api.post(
            f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
            data=trainset.SerializeToString(),
        ) as response:
            assert response.status == 200
            batches = []
            total = 0
            async for batch in get_field_classification_batch_from_response(response):
                batches.append(batch)
                total += len(batch.data)
            assert len(batches) == expected_batches
            assert total == expected_total


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ["STABLE", "EXPERIMENTAL"], indirect=True)
async def test_generator_field_classification_populates_ids(
    train_rest_api: aiohttp.ClientSession,
    knowledgebox_with_labels: str,
):
    kbid = knowledgebox_with_labels

    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.FIELD_CLASSIFICATION
    trainset.batch_size = 2

    async with train_rest_api.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        total = 0
        titles = 0
        summaries = 0
        async for batch in get_field_classification_batch_from_response(response):
            batches.append(batch)
            total += len(batch.data)

            for data in batch.data:
                field_id = data.id
                if field_id.endswith("title"):
                    titles += 1
                if field_id.endswith("summary"):
                    summaries += 1

        assert len(batches) == 2
        assert total == 4
        assert titles == 2
        assert summaries == 2


@pytest.fixture(scope="function")
@pytest.mark.asyncio
async def knowledgebox_with_labels(nucliadb_grpc: WriterStub, knowledgebox: str):
    slr = SetLabelsRequest()
    slr.kb.uuid = knowledgebox
    slr.id = "labelset_paragraphs"
    slr.labelset.kind.append(LabelSet.LabelSetKind.PARAGRAPHS)
    slr.labelset.labels.append(Label(title="label_machine"))
    slr.labelset.labels.append(Label(title="label_user"))
    slr.labelset.labels.append(Label(title="label_alien"))
    await nucliadb_grpc.SetLabels(slr)  # type: ignore

    slr = SetLabelsRequest()
    slr.kb.uuid = knowledgebox
    slr.id = "labelset_resources"
    slr.labelset.kind.append(LabelSet.LabelSetKind.RESOURCES)
    slr.labelset.labels.append(Label(title="label_machine"))
    slr.labelset.labels.append(Label(title="label_user"))
    slr.labelset.labels.append(Label(title="label_alien"))
    await nucliadb_grpc.SetLabels(slr)  # type: ignore

    bmb = BrokerMessageBuilder(kbid=knowledgebox)
    bmb.with_title("First resource")
    bmb.with_summary("First summary")
    bmb.with_resource_labels("labelset_resources", ["label_user"])
    bm = bmb.build()
    await inject_message(nucliadb_grpc, bm)

    bmb = BrokerMessageBuilder(kbid=knowledgebox)
    bmb.with_title("Second resource")
    bmb.with_summary("Second summary")
    bmb.with_resource_labels("labelset_resources", ["label_machine"])
    bm = bmb.build()
    await inject_message(nucliadb_grpc, bm)

    await asyncio.sleep(0.1)
    yield knowledgebox


@pytest.fixture(scope="function")
@pytest.mark.asyncio
async def knowledgebox_without_labels(knowledgebox: str, nucliadb_grpc: WriterStub):
    bmb = BrokerMessageBuilder(kbid=knowledgebox)
    bmb.with_title("First resource")
    bmb.with_summary("First summary")
    bm = bmb.build()
    await inject_message(nucliadb_grpc, bm)

    bmb = BrokerMessageBuilder(kbid=knowledgebox)
    bmb.with_title("Second resource")
    bmb.with_summary("Second summary")
    bm = bmb.build()
    await inject_message(nucliadb_grpc, bm)

    await asyncio.sleep(0.1)
    yield knowledgebox
