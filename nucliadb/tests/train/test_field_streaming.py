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


import aiohttp
import pytest
from httpx import AsyncClient

from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb_models.filters import FilterExpression, Label
from nucliadb_models.trainset import TrainSet as TrainSetModel
from nucliadb_models.trainset import TrainSetType
from nucliadb_protos.dataset_pb2 import (
    FieldStreamingBatch,
    TaskType,
    TrainSet,
)
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import smb_wonder_resource
from tests.train.utils import get_batches_from_train_response_stream


def test_model():
    # Should not work unless field streaming type is selected
    with pytest.raises(ValueError):
        TrainSetModel(
            type=TrainSetType.IMAGE_CLASSIFICATION,
            filter_expression=FilterExpression(field=Label(labelset="foo")),
        )
    TrainSetModel(
        type=TrainSetType.FIELD_STREAMING,
        filter_expression=FilterExpression(field=Label(labelset="foo")),
    )


@pytest.mark.deploy_modes("standalone")
async def test_generator_field_streaming_legacy(
    nucliadb_train: aiohttp.ClientSession,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
):
    kbid = knowledgebox

    await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    async with nucliadb_train.get(f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset") as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.FIELD_STREAMING
    trainset.batch_size = 5

    async with nucliadb_train.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        async for batch in get_batches_from_train_response_stream(response, FieldStreamingBatch):
            batches.append(batch)
            assert len(batch.data) == 3
            for field_split_data in batch.data:
                assert field_split_data.text.text
        assert len(batches) == 1

    # Try now excluding the text serialization
    trainset.exclude_text = True
    async with nucliadb_train.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        async for batch in get_batches_from_train_response_stream(response, FieldStreamingBatch):
            batches.append(batch)
            assert len(batch.data) == 3
            for field_split_data in batch.data:
                assert field_split_data.text.text == ""
        assert len(batches) == 1


@pytest.mark.deploy_modes("standalone")
async def test_generator_field_streaming_json(
    nucliadb_train: aiohttp.ClientSession,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
):
    kbid = knowledgebox

    await smb_wonder_resource(kbid, nucliadb_writer, nucliadb_ingest_grpc)

    async with nucliadb_train.get(f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset") as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSetModel(
        type=TrainSetType.FIELD_STREAMING,
        batch_size=5,
        exclude_text=False,
    )

    stream_partition_url = f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}"

    async with nucliadb_train.post(
        stream_partition_url,
        json=trainset.model_dump(),
    ) as response:
        assert response.status == 200
        batches = []
        async for batch in get_batches_from_train_response_stream(response, FieldStreamingBatch):
            batches.append(batch)
            assert len(batch.data) == 3
            for field_split_data in batch.data:
                assert field_split_data.text.text
        assert len(batches) == 1

    # Try now excluding the text serialization
    trainset.exclude_text = True
    async with nucliadb_train.post(stream_partition_url, json=trainset.model_dump()) as response:
        assert response.status == 200
        batches = []
        async for batch in get_batches_from_train_response_stream(response, FieldStreamingBatch):
            batches.append(batch)
            assert len(batch.data) == 3
            for field_split_data in batch.data:
                assert field_split_data.text.text == ""
        assert len(batches) == 1

    # Test that wrong payloads are validated
    for post_kwargs in (
        {"data": b"fppp*}", "headers": {"Content-Type": "application/json"}},
        {"json": {"type": 50}},
        {"data": b"foobar"},
    ):
        resp = await nucliadb_train.post(stream_partition_url, **post_kwargs)
        assert resp.status == 422
