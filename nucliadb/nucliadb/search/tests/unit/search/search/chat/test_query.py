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
import asyncio
import random
from typing import AsyncIterator
from unittest import mock

import pytest

from nucliadb.search.search.chat.query import audited_generator
from nucliadb_models.search import NucliaDBClientType


async def predict_generator() -> AsyncIterator[bytes]:
    answer_chunks = ["This is the ", "answer to your", " question"]
    for chunk in answer_chunks:
        await asyncio.sleep(random.uniform(0, 0.2))
        yield chunk.encode()


@pytest.mark.asyncio
async def test_audited_generator():
    audit_storage = mock.AsyncMock()
    chat_request = mock.Mock(query="This is my question")

    answer_generator = audited_generator(
        predict_generator(),
        "kbid",
        "user_id",
        NucliaDBClientType.DESKTOP,
        "origin",
        chat_request=chat_request,
        audit=audit_storage,
    )

    answer = []
    async for part in answer_generator:
        answer.append(part)

    # Check that answer is generated correctly
    assert b"".join(answer) == b"This is the answer to your question"

    # Check that audit is called
    audit_storage.chat.assert_awaited_once()
    call_args = audit_storage.chat.call_args[0]
    call_kwargs = audit_storage.chat.call_args[1]
    assert call_args[0] == "kbid"
    assert call_kwargs["question"] == "This is my question"
    assert call_kwargs["answer"] == "This is the answer to your question"


async def predict_generator_no_context() -> AsyncIterator[bytes]:
    answer_chunks = ["Not enough data to answer", " this."]
    for chunk in answer_chunks:
        await asyncio.sleep(random.uniform(0, 0.1))
        yield chunk.encode()


@pytest.mark.asyncio
async def test_audited_generator_no_context():
    audit_storage = mock.AsyncMock()
    answer_generator = audited_generator(
        predict_generator_no_context(),
        "kbid",
        "user_id",
        NucliaDBClientType.DESKTOP,
        "origin",
        chat_request=mock.Mock(query="This is my question"),
        audit=audit_storage,
    )
    # Iterate the generator
    [_ async for _ in answer_generator]

    # Check that audit is called
    call_kwargs = audit_storage.chat.call_args[1]
    assert call_kwargs["answer"] is None
