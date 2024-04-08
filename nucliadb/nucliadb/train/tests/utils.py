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

from typing import AsyncGenerator, overload

import aiohttp
from nucliadb_protos.dataset_pb2 import (
    FieldClassificationBatch,
    ImageClassificationBatch,
    ParagraphClassificationBatch,
    ParagraphStreamingBatch,
    QuestionAnswerStreamingBatch,
    SentenceClassificationBatch,
    TokenClassificationBatch,
)

from nucliadb.train.types import TrainBatch, TrainBatchType

# NOTE: we use def instead of async def to make mypy happy. Otherwise, it
# considers the overloaded functions as corountines returning async iterators
# instead of async iterators themselves and complains about it


@overload
def get_batches_from_train_response_stream(
    response: aiohttp.ClientResponse,
    pb_klass: type[FieldClassificationBatch],
) -> AsyncGenerator[FieldClassificationBatch, None]:
    ...


@overload
def get_batches_from_train_response_stream(
    response: aiohttp.ClientResponse,
    pb_klass: type[ImageClassificationBatch],
) -> AsyncGenerator[ImageClassificationBatch, None]:
    ...


@overload
def get_batches_from_train_response_stream(
    response: aiohttp.ClientResponse,
    pb_klass: type[ParagraphClassificationBatch],
) -> AsyncGenerator[ParagraphClassificationBatch, None]:
    ...


@overload
def get_batches_from_train_response_stream(
    response: aiohttp.ClientResponse,
    pb_klass: type[ParagraphStreamingBatch],
) -> AsyncGenerator[ParagraphStreamingBatch, None]:
    ...


@overload
def get_batches_from_train_response_stream(
    response: aiohttp.ClientResponse,
    pb_klass: type[QuestionAnswerStreamingBatch],
) -> AsyncGenerator[QuestionAnswerStreamingBatch, None]:
    ...


@overload
def get_batches_from_train_response_stream(
    response: aiohttp.ClientResponse,
    pb_klass: type[SentenceClassificationBatch],
) -> AsyncGenerator[SentenceClassificationBatch, None]:
    ...


@overload
def get_batches_from_train_response_stream(
    response: aiohttp.ClientResponse,
    pb_klass: type[TokenClassificationBatch],
) -> AsyncGenerator[TokenClassificationBatch, None]:
    ...


async def get_batches_from_train_response_stream(
    response: aiohttp.ClientResponse,
    pb_klass: TrainBatchType,
) -> AsyncGenerator[TrainBatch, None]:
    while True:
        header = await response.content.read(4)
        if header == b"":
            break
        payload_size = int.from_bytes(header, byteorder="big", signed=False)
        payload = await response.content.read(payload_size)
        batch = pb_klass()
        batch.ParseFromString(payload)
        yield batch
