# Copyright (C) 2024 Bosutech XXI S.L.
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

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, List

import pyarrow as pa  # type: ignore

from nucliadb_dataset.mapping import (
    batch_to_image_classification_arrow,
    batch_to_paragraph_streaming_arrow,
    batch_to_question_answer_streaming_arrow,
    batch_to_text_classification_arrow,
    batch_to_text_classification_normalized_arrow,
    batch_to_token_classification_arrow,
    bytes_to_batch,
)
from nucliadb_protos.dataset_pb2 import (
    FieldClassificationBatch,
    ImageClassificationBatch,
    ParagraphClassificationBatch,
    ParagraphStreamingBatch,
    QuestionAnswerStreamingBatch,
    SentenceClassificationBatch,
    TaskType,
    TokenClassificationBatch,
)

if TYPE_CHECKING:  # pragma: no cover
    TaskValue = TaskType.V
else:
    TaskValue = int

ACTUAL_PARTITION = "actual_partition"


class Task(str, Enum):
    PARAGRAPH_CLASSIFICATION = "PARAGRAPH_CLASSIFICATION"
    FIELD_CLASSIFICATION = "FIELD_CLASSIFICATION"
    SENTENCE_CLASSIFICATION = "SENTENCE_CLASSIFICATION"
    TOKEN_CLASSIFICATION = "TOKEN_CLASSIFICATION"
    IMAGE_CLASSIFICATION = "IMAGE_CLASSIFICATION"
    PARAGRAPH_STREAMING = "PARAGRAPH_STREAMING"
    QUESTION_ANSWER_STREAMING = "QUESTION_ANSWER_STREAMING"


@dataclass
class TaskDefinition:
    schema: pa.schema
    proto: Any
    labels: bool
    mapping: List[Callable]


TASK_DEFINITIONS: Dict[Task, TaskDefinition] = {
    Task.PARAGRAPH_CLASSIFICATION: TaskDefinition(
        schema=pa.schema(
            [
                pa.field("text", pa.string()),
                pa.field("labels", pa.list_(pa.string())),
            ]
        ),
        mapping=[
            bytes_to_batch(ParagraphClassificationBatch),
            batch_to_text_classification_arrow(),
        ],
        proto=TaskType.PARAGRAPH_CLASSIFICATION,
        labels=True,
    ),
    Task.FIELD_CLASSIFICATION: TaskDefinition(
        schema=pa.schema(
            [
                pa.field("text", pa.string()),
                pa.field("labels", pa.list_(pa.string())),
            ]
        ),
        mapping=[
            bytes_to_batch(FieldClassificationBatch),
            batch_to_text_classification_arrow(),
        ],
        proto=TaskType.FIELD_CLASSIFICATION,
        labels=True,
    ),
    Task.SENTENCE_CLASSIFICATION: TaskDefinition(
        schema=pa.schema(
            [
                pa.field("text", pa.string()),
                pa.field("labels", pa.list_(pa.string())),
            ]
        ),
        mapping=[
            bytes_to_batch(SentenceClassificationBatch),
            batch_to_text_classification_normalized_arrow(),
        ],
        proto=TaskType.SENTENCE_CLASSIFICATION,
        labels=True,
    ),
    Task.TOKEN_CLASSIFICATION: TaskDefinition(
        schema=pa.schema(
            [
                pa.field("text", pa.list_(pa.string())),
                pa.field("labels", pa.list_(pa.string())),
            ]
        ),
        mapping=[
            bytes_to_batch(TokenClassificationBatch),
            batch_to_token_classification_arrow(),
        ],
        proto=TaskType.TOKEN_CLASSIFICATION,
        labels=True,
    ),
    Task.IMAGE_CLASSIFICATION: TaskDefinition(
        schema=pa.schema(
            [
                pa.field("image", pa.string()),
                pa.field("selection", pa.string()),
            ]
        ),
        mapping=[
            bytes_to_batch(ImageClassificationBatch),
            batch_to_image_classification_arrow(),
        ],
        proto=TaskType.IMAGE_CLASSIFICATION,
        labels=False,
    ),
    Task.PARAGRAPH_STREAMING: TaskDefinition(
        schema=pa.schema(
            [
                pa.field("paragraph_id", pa.string()),
                pa.field("text", pa.string()),
            ]
        ),
        mapping=[
            bytes_to_batch(ParagraphStreamingBatch),
            batch_to_paragraph_streaming_arrow(),
        ],
        proto=TaskType.PARAGRAPH_STREAMING,
        labels=False,
    ),
    Task.QUESTION_ANSWER_STREAMING: TaskDefinition(
        schema=pa.schema(
            [
                pa.field("question", pa.string()),
                pa.field("answer", pa.string()),
                pa.field("question_paragraphs", pa.list_(pa.string())),
                pa.field("answer_paragraphs", pa.list_(pa.string())),
                pa.field("question_language", pa.string()),
                pa.field("answer_language", pa.string()),
                pa.field("cancelled_by_user", pa.bool_()),
            ]
        ),
        mapping=[
            bytes_to_batch(QuestionAnswerStreamingBatch),
            batch_to_question_answer_streaming_arrow(),
        ],
        proto=TaskType.QUESTION_ANSWER_STREAMING,
        labels=False,
    ),
}

TASK_DEFINITIONS_REVERSE = {task.proto: task for task in TASK_DEFINITIONS.values()}  # noqa
