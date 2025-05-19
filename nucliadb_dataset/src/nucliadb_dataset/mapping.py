# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Mapping to transform a text/labels onto tokens and multilabelbinarizer
from typing import Any, TypeVar

import pyarrow as pa  # type: ignore

from nucliadb_protos.dataset_pb2 import (
    FieldClassificationBatch,
    FieldStreamingBatch,
    ImageClassificationBatch,
    ParagraphClassificationBatch,
    ParagraphStreamingBatch,
    QuestionAnswerStreamingBatch,
    SentenceClassificationBatch,
    TokenClassificationBatch,
)

BatchType = TypeVar("BatchType", ParagraphClassificationBatch, FieldClassificationBatch)


def bytes_to_batch(klass: Any):
    def func(batch: bytes, *args, **kwargs) -> Any:
        pb = klass()
        pb.ParseFromString(batch)
        return pb

    return func


def batch_to_text_classification_arrow():
    def func(batch: BatchType, schema: pa.schema):
        TEXT = []
        LABELS = []
        for data in batch.data:
            if data.text:
                TEXT.append(data.text)
                LABELS.append([f"{label.labelset}/{label.label}" for label in data.labels])
        if len(TEXT):
            pa_data = [pa.array(TEXT), pa.array(LABELS)]
            output_batch = pa.record_batch(pa_data, schema=schema)
        else:
            output_batch = None
        return output_batch

    return func


def batch_to_token_classification_arrow():
    def func(batch: TokenClassificationBatch, schema: pa.schema):
        X = []
        Y = []
        for data in batch.data:
            X.append([x for x in data.token])
            Y.append([x for x in data.label])
        if len(X):
            pa_data = [pa.array(X), pa.array(Y)]
            output_batch = pa.record_batch(pa_data, schema)
        else:
            output_batch = None
        return output_batch

    return func


def batch_to_text_classification_normalized_arrow():
    def func(batch: SentenceClassificationBatch, schema: pa.schema):
        TEXT = []
        LABELS = []
        for data in batch.data:
            batch_labels = [f"{label.labelset}/{label.label}" for label in data.labels]
            for sentence in data.text:
                TEXT.append(sentence)
                LABELS.append(batch_labels)
        if len(TEXT):
            pa_data = [pa.array(TEXT), pa.array(LABELS)]
            output_batch = pa.record_batch(pa_data, schema=schema)
        else:
            output_batch = None
        return output_batch

    return func


def batch_to_image_classification_arrow():
    def func(batch: ImageClassificationBatch, schema: pa.schema):
        IMAGE = []
        SELECTION = []
        for data in batch.data:
            IMAGE.append(data.page_uri)
            SELECTION.append(data.selections)

        if len(IMAGE):
            pa_data = [pa.array(IMAGE), pa.array(SELECTION)]
            output_batch = pa.record_batch(pa_data, schema=schema)
        else:
            output_batch = None
        return output_batch

    return func


def batch_to_paragraph_streaming_arrow():
    def func(batch: ParagraphStreamingBatch, schema: pa.schema):
        PARARGAPH_ID = []
        TEXT = []
        for data in batch.data:
            PARARGAPH_ID.append(data.id)
            TEXT.append(data.text)

        if len(PARARGAPH_ID):
            pa_data = [pa.array(PARARGAPH_ID), pa.array(TEXT)]
            output_batch = pa.record_batch(pa_data, schema=schema)
        else:
            output_batch = None
        return output_batch

    return func


def batch_to_question_answer_streaming_arrow():
    def func(batch: QuestionAnswerStreamingBatch, schema: pa.schema):
        QUESTION = []
        ANSWER = []
        QUESTION_PARAGRAPHS = []
        ANSWER_PARAGRAPHS = []
        QUESTION_LANGUAGE = []
        ANSWER_LANGUAGE = []
        CANCELLED_BY_USER = []
        for data in batch.data:
            QUESTION.append(data.question.text)
            ANSWER.append(data.answer.text)
            QUESTION_PARAGRAPHS.append([paragraph for paragraph in data.question.paragraphs])
            ANSWER_PARAGRAPHS.append([paragraph for paragraph in data.answer.paragraphs])
            QUESTION_LANGUAGE.append(data.question.language)
            ANSWER_LANGUAGE.append(data.answer.language)
            CANCELLED_BY_USER.append(data.cancelled_by_user)

        if len(QUESTION):
            pa_data = [
                pa.array(QUESTION),
                pa.array(ANSWER),
                pa.array(QUESTION_PARAGRAPHS),
                pa.array(ANSWER_PARAGRAPHS),
                pa.array(QUESTION_LANGUAGE),
                pa.array(ANSWER_LANGUAGE),
                pa.array(CANCELLED_BY_USER),
            ]
            output_batch = pa.record_batch(pa_data, schema=schema)
        else:
            output_batch = None
        return output_batch

    return func


def batch_to_field_streaming_arrow():
    def func(batch: FieldStreamingBatch, schema: pa.schema):
        SPLIT = []
        RID = []
        FIELD = []
        FIELD_TYPE = []
        LABELS = []
        EXTRACTED_TEXT = []
        METADATA = []
        BASIC = []
        for data in batch.data:
            SPLIT.append(data.split)
            RID.append(data.rid)
            FIELD.append(data.field)
            FIELD_TYPE.append(data.field_type)
            LABELS.append([x for x in data.labels])
            EXTRACTED_TEXT.append(data.text.SerializeToString())
            METADATA.append(data.metadata.SerializeToString())
            BASIC.append(data.basic.SerializeToString())
        if len(RID):
            pa_data = [
                pa.array(SPLIT),
                pa.array(RID),
                pa.array(FIELD),
                pa.array(FIELD_TYPE),
                pa.array(LABELS),
                pa.array(EXTRACTED_TEXT),
                pa.array(BASIC),
                pa.array(METADATA),
            ]
            output_batch = pa.record_batch(pa_data, schema)
        else:
            output_batch = None
        return output_batch

    return func
