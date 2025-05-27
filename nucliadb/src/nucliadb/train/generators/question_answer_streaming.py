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

from typing import AsyncGenerator, Optional

from nidx_protos.nodereader_pb2 import StreamRequest

from nucliadb.common.ids import FIELD_TYPE_PB_TO_STR, FIELD_TYPE_STR_TO_PB
from nucliadb.common.nidx import get_nidx_searcher_client
from nucliadb.train import logger
from nucliadb.train.generators.utils import (
    batchify,
    get_paragraph,
    get_resource_from_cache_or_db,
)
from nucliadb_models.filters import FilterExpression
from nucliadb_protos.dataset_pb2 import (
    QuestionAnswerStreamingBatch,
    QuestionAnswerStreamItem,
    TrainSet,
)
from nucliadb_protos.resources_pb2 import (
    FieldID,
    QuestionAnswer,
    QuestionAnswerAnnotation,
)


def question_answer_batch_generator(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
    filter_expression: Optional[FilterExpression],
) -> AsyncGenerator[QuestionAnswerStreamingBatch, None]:
    generator = generate_question_answer_streaming_payloads(kbid, trainset, shard_replica_id)
    batch_generator = batchify(generator, trainset.batch_size, QuestionAnswerStreamingBatch)
    return batch_generator


async def generate_question_answer_streaming_payloads(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
):
    request = StreamRequest()
    request.shard_id.id = shard_replica_id

    async for document_item in get_nidx_searcher_client().Documents(request):
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

        field_type_int = FIELD_TYPE_STR_TO_PB[field_type]
        field_obj = await orm_resource.get_field(field, field_type_int, load=False)

        question_answers_pb = await field_obj.get_question_answers()
        if question_answers_pb is not None:
            for question_answer_pb in question_answers_pb.question_answers.question_answer:
                async for item in iter_stream_items(kbid, question_answer_pb):
                    yield item
            for question_answer_pb in question_answers_pb.split_question_answers.values():
                for split_question_answer_pb in question_answer_pb.question_answer:
                    async for item in iter_stream_items(kbid, split_question_answer_pb):
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
        except Exception as exc:  # pragma: no cover
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
            except Exception as exc:  # pragma: no cover
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
    return field.field == field_id and FIELD_TYPE_PB_TO_STR[field.field_type] == field_type
