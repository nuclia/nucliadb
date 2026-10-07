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

from collections.abc import AsyncGenerator

from nidx_protos.nodereader_pb2 import StreamRequest

from nucliadb.common.ids import FIELD_TYPE_PB_TO_STR
from nucliadb.train import logger
from nucliadb.train.generators.utils import (
    batchify,
    get_paragraph,
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
)


def question_answer_batch_generator(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
    filter_expression: FilterExpression | None,
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

    # TODO(Marklogic): Implement iterating documents (fields)
    if False:
        yield


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
