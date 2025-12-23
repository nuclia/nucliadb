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

from collections.abc import AsyncIterator

import aiohttp
import pytest
from httpx import AsyncClient

from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb_protos.dataset_pb2 import QuestionAnswerStreamingBatch, TaskType, TrainSet
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.ndbfixtures.resources import smb_wonder_resource
from tests.train.utils import get_batches_from_train_response_stream


async def get_question_answer_streaming_batch_from_response(
    response: aiohttp.ClientResponse,
) -> AsyncIterator[QuestionAnswerStreamingBatch]:
    while True:
        header = await response.content.read(4)
        if header == b"":
            break
        payload_size = int.from_bytes(header, byteorder="big", signed=False)
        payload = await response.content.read(payload_size)
        pcb = QuestionAnswerStreamingBatch()
        pcb.ParseFromString(payload)
        assert pcb.data
        yield pcb


@pytest.mark.deploy_modes("standalone")
async def test_generator_question_answer_streaming(
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
    trainset.type = TaskType.QUESTION_ANSWER_STREAMING
    trainset.batch_size = 5
    trainset.filter.labels.append("labelset_paragraphs")

    async with nucliadb_train.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        questions = []
        answers = []
        question_paragraphs_count = 0
        answer_paragraphs_count = 0
        async for batch in get_batches_from_train_response_stream(
            response, QuestionAnswerStreamingBatch
        ):
            batches.append(batch)
            assert len(batch.data) == 3
            for data in batch.data:
                questions.append(data.question.text)
                question_paragraphs_count += len(data.question.paragraphs)
                answers.append(data.answer.text)
                answer_paragraphs_count += len(data.answer.paragraphs)
        assert len(batches) == 1
        assert len(questions) == len(answers) == 3
        assert len(set(questions)) == 2
        assert question_paragraphs_count == 2
        assert answer_paragraphs_count == 4
