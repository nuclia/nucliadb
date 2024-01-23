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
from typing import AsyncIterator

import aiohttp
import pytest
from nucliadb_protos.dataset_pb2 import QuestionAnswerStreamingBatch, TaskType, TrainSet
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.ingest.orm.resource import FIELD_TYPE_TO_ID
from nucliadb.tests.utils import inject_message
from nucliadb.tests.utils.broker_messages import BrokerMessageBuilder, FieldBuilder
from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb.train.tests.utils import get_batches_from_train_response_stream
from nucliadb_protos import resources_pb2 as rpb


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


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ["STABLE", "EXPERIMENTAL"], indirect=True)
async def test_generator_question_answer_streaming(
    train_rest_api: aiohttp.ClientSession,
    nucliadb_grpc: WriterStub,
    knowledgebox: str,
):
    kbid = knowledgebox

    await inject_resources_with_question_answers(kbid, nucliadb_grpc)

    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.QUESTION_ANSWER_STREAMING
    trainset.batch_size = 5
    trainset.filter.labels.append("labelset_paragraphs")

    async with train_rest_api.post(
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


async def inject_resources_with_question_answers(kbid: str, nucliadb_grpc: WriterStub):
    await inject_message(nucliadb_grpc, smb_wonder_bm(kbid))
    await asyncio.sleep(0.1)


def smb_wonder_bm(kbid: str) -> BrokerMessage:
    rid = str(uuid.uuid4())
    bmb = BrokerMessageBuilder(kbid=kbid, rid=rid)
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

    start = 0
    end = len(paragraphs[0])
    paragraph_0_id = (
        f"{rid}/{FIELD_TYPE_TO_ID[rpb.FieldType.FILE]}/smb-wonder/{start}-{end}"
    )

    start = len(paragraphs[0])
    end = len(paragraphs[0]) + len(paragraphs[1])
    paragraph_1_id = (
        f"{rid}/{FIELD_TYPE_TO_ID[rpb.FieldType.FILE]}/smb-wonder/{start}-{end}"
    )

    question = "What is SMB Wonder?"
    field_builder.add_question_answer(
        question=question,
        question_paragraph_ids=[paragraph_0_id],
        answer="SMB Wonder is a side-scrolling Nintendo Switch game",
        answer_paragraph_ids=[paragraph_0_id, paragraph_1_id],
    )
    field_builder.add_question_answer(
        question=question,
        question_paragraph_ids=[paragraph_0_id],
        answer="It's the new Mario game for Nintendo Switch",
        answer_paragraph_ids=[paragraph_0_id],
    )

    question = "Give me an example of side-scrolling game"
    field_builder.add_question_answer(
        question=question,
        answer="SMB Wonder game",
        answer_paragraph_ids=[paragraph_1_id],
    )

    bmb.add_field_builder(field_builder)

    bm = bmb.build()

    return bm


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ["STABLE", "EXPERIMENTAL"], indirect=True)
async def test_generator_question_answer_streaming_streams_qa_annotations(
    train_rest_api: aiohttp.ClientSession,
    writer_rest_api: aiohttp.ClientSession,
    knowledgebox: str,
):
    kbid = knowledgebox

    resp = await writer_rest_api.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/resources",
        json={
            "title": "Super Mario Bros. Wonder",
            "texts": {
                "smb-wonder": {
                    "body": "Super Mario Bros. Wonder (SMB Wonder) is a 2023 platform game developed and published by Nintendo.\n"  # noqa
                },
            },
            "fieldmetadata": [
                {
                    "field": {"field_type": "text", "field": "smb-wonder"},
                    "question_answers": [
                        {
                            "cancelled_by_user": True,
                            "question_answer": {
                                "question": {
                                    "text": "What is SMB Wonder?",
                                    "ids_paragraphs": [],
                                },
                                "answers": [
                                    {
                                        "ids_paragraphs": [],
                                        "language": "english",
                                        "text": "SMB Wonder is a Nintendo Switch game",
                                    }
                                ],
                            },
                        }
                    ],
                }
            ],
        },
    )
    assert resp.status == 201, resp.text

    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type = TaskType.QUESTION_ANSWER_STREAMING
    trainset.batch_size = 5

    async with train_rest_api.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{kbid}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:
        assert response.status == 200
        batches = []
        async for batch in get_batches_from_train_response_stream(
            response, QuestionAnswerStreamingBatch
        ):
            batches.append(batch)
            assert len(batch.data) == 1
        assert len(batches) == 1
