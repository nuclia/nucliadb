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
#

from typing import AsyncGenerator

from nucliadb_protos.dataset_pb2 import (
    Paragraph,
    QuestionAnswerStreamingBatch,
    QuestionAnswerStreamItem,
    TrainSet,
)
from nucliadb_protos.nodereader_pb2 import StreamRequest

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.train import logger
from nucliadb.train.generators.utils import (
    batchify,
    get_paragraph,
    get_resource_from_cache_or_db,
)


def question_answer_batch_generator(
    kbid: str,
    trainset: TrainSet,
    node: AbstractIndexNode,
    shard_replica_id: str,
) -> AsyncGenerator[QuestionAnswerStreamingBatch, None]:
    generator = generate_question_answer_streaming_payloads(
        kbid, trainset, node, shard_replica_id
    )
    batch_generator = batchify(
        generator, trainset.batch_size, QuestionAnswerStreamingBatch
    )
    return batch_generator


async def generate_question_answer_streaming_payloads(
    kbid: str,
    trainset: TrainSet,
    node: AbstractIndexNode,
    shard_replica_id: str,
):
    request = StreamRequest()
    request.shard_id.id = shard_replica_id

    async for document_item in node.stream_get_fields(request):
        field_id = f"{document_item.uuid}{document_item.field}"
        rid, field_type, field = field_id.split("/")

        orm_resource = await get_resource_from_cache_or_db(kbid, rid)
        if orm_resource is None:
            logger.error(f"{rid} does not exist on DB")
            continue

        field_type_int = KB_REVERSE[field_type]
        field_obj = await orm_resource.get_field(field, field_type_int, load=False)

        question_answers_pb = await field_obj.get_question_answers()
        if question_answers_pb is not None:
            for question_answer_pb in question_answers_pb.question_answer:
                question_pb = question_answer_pb.question
                for answer_pb in question_answer_pb.answers:
                    item = QuestionAnswerStreamItem()
                    item.question.text = question_pb.text
                    item.question.language = question_pb.language
                    item.answer.text = answer_pb.text
                    item.answer.language = answer_pb.language
                    for paragraph_id in answer_pb.ids_paragraphs:
                        paragraph_pb = Paragraph()
                        paragraph_pb.id = paragraph_id
                        paragraph_pb.text = await get_paragraph(kbid, paragraph_id)
                        item.paragraphs.append(paragraph_pb)

                        yield item
