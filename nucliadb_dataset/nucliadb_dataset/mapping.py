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

# Mapping to transform a text/labels onto tokens and multilabelbinarizer
from typing import Any, TypeVar

import pyarrow as pa  # type: ignore
from nucliadb_protos.dataset_pb2 import (
    FieldClassificationBatch,
    ImageClassificationBatch,
    ParagraphClassificationBatch,
    SentenceClassificationBatch,
    TokenClassificationBatch,
)

BatchType = TypeVar("BatchType", ParagraphClassificationBatch, FieldClassificationBatch)


def bytes_to_batch(klass: Any):
    def func(batch: bytes) -> Any:
        pb = klass()
        pb.ParseFromString(batch)
        return pb

    return func


def batch_to_text_classification_arrow(batch: BatchType):
    X = []
    Y = []
    for data in batch.data:
        if data.text:
            X.append(data.text)
            Y.append([f"{label.labelset}/{label.label}" for label in data.labels])
    if len(X):
        pa_data = [pa.array(X), pa.array(Y)]
        output_batch = pa.record_batch(pa_data, names=["text", "labels"])
    else:
        output_batch = None
    return output_batch


def batch_to_token_classification_arrow(schema: pa.schema):
    def func(batch: TokenClassificationBatch):
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


def batch_to_text_classification_normalized_arrow(batch: SentenceClassificationBatch):
    TEXT = []
    LABELS = []
    for data in batch.data:
        batch_labels = [f"{label.labelset}/{label.label}" for label in data.labels]
        for sentence in data.text:
            TEXT.append(sentence)
            LABELS.append(batch_labels)
    if len(TEXT):
        pa_data = [pa.array(TEXT), pa.array(LABELS)]
        output_batch = pa.record_batch(pa_data, names=["text", "labels"])
    else:
        output_batch = None
    return output_batch


def batch_to_image_classification_arrow(batch: ImageClassificationBatch):
    IMAGE = []
    SELECTION = []
    for data in batch.data:
        IMAGE.append(data.page_uri)
        SELECTION.append(data.selections)

    if len(IMAGE):
        pa_data = [pa.array(IMAGE), pa.array(SELECTION)]
        output_batch = pa.record_batch(pa_data, names=["image", "selection"])
    else:
        output_batch = None
    return output_batch
