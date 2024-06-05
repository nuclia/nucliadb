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
    QuestionAnswerStreamingBatch,
    QuestionAnswerStreamItem,
    TrainSet,
)
from nucliadb_protos.nodereader_pb2 import StreamRequest
from nucliadb_protos.resources_pb2 import (
    FieldID,
    QuestionAnswer,
    QuestionAnswerAnnotation,
)

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.ingest.orm.resource import FIELD_TYPE_TO_ID, KB_REVERSE
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

        basic = await orm_resource.get_basic()
        if basic is not None:
            for field_metadata in basic.fieldmetadata:
                if not is_same_field(field_metadata.field, field, field_type):
                    continue
                qa_annotation_pb: QuestionAnswerAnnotation
                for qa_annotation_pb in field_metadata.question_answers:
                    async for item in iter_stream_items(
                        kbid,
                        qa_annotation_pb.question_answer,
                    ):
                        # QA annotations may be cancelled by user
                        item.cancelled_by_user = qa_annotation_pb.cancelled_by_user
                        yield item

        field_type_int = KB_REVERSE[field_type]
        field_obj = await orm_resource.get_field(field, field_type_int, load=False)

        question_answers_pb = await field_obj.get_question_answers()
        if question_answers_pb is not None:
            for question_answer_pb in question_answers_pb.question_answer:
                async for item in iter_stream_items(kbid, question_answer_pb):
                    yield item


async def iter_stream_items(
    kbid: str,
    question_answer_pb: QuestionAnswer,
) -> AsyncGenerator[QuestionAnswerStreamItem, None]:
    question_pb = question_answer_pb.question
    question_paragraphs = []
    for paragraph_id in question_pb.ids_paragraphs:
        try:
            text = await get_paragraph(kbid, paragraph_id)
        except Exception as exc:  # pragma: nocover
            logger.warning(
                "Question paragraph couldn't be fetched while streaming Q&A",
                extra={"kbid": kbid, "paragraph_id": paragraph_id},
                exc_info=exc,
            )
        else:
            if text:
                question_paragraphs.append(text)
    for answer_pb in question_answer_pb.answers:
        item = QuestionAnswerStreamItem()
        item.question.text = question_pb.text
        item.question.language = question_pb.language
        item.question.paragraphs.extend(question_paragraphs)
        item.answer.text = answer_pb.text
        item.answer.language = answer_pb.language
        for paragraph_id in answer_pb.ids_paragraphs:
            try:
                text = await get_paragraph(kbid, paragraph_id)
            except Exception as exc:  # pragma: nocover
                logger.warning(
                    "Answer paragraph couldn't be fetched while streaming Q&A",
                    extra={"kbid": kbid, "paragraph_id": paragraph_id},
                    exc_info=exc,
                )
            else:
                if text:
                    item.answer.paragraphs.append(text)
        yield item


def is_same_field(field: FieldID, field_id: str, field_type: str) -> bool:
    return field.field == field_id and FIELD_TYPE_TO_ID[field.field_type] == field_type
