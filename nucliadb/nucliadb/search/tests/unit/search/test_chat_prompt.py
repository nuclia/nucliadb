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
from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.search.search.chat import prompt as chat_prompt
from nucliadb_models.search import (
    SCORE_TYPE,
    FindField,
    FindParagraph,
    FindResource,
    KnowledgeboxFindResults,
)
from nucliadb_protos import resources_pb2

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def messages():
    msgs = [
        resources_pb2.Message(
            ident="1", content=resources_pb2.MessageContent(text="Message 1")
        ),
        resources_pb2.Message(
            ident="2", content=resources_pb2.MessageContent(text="Message 2")
        ),
        resources_pb2.Message(
            ident="3",
            who="1",
            content=resources_pb2.MessageContent(text="Message 3"),
            type=resources_pb2.Message.MessageType.QUESTION,
        ),
        resources_pb2.Message(
            ident="4",
            content=resources_pb2.MessageContent(text="Message 4"),
            type=resources_pb2.Message.MessageType.ANSWER,
            to=["1"],
        ),
        resources_pb2.Message(
            ident="5", content=resources_pb2.MessageContent(text="Message 5")
        ),
    ]
    yield msgs


@pytest.fixture()
def field_obj(messages):
    mock = AsyncMock()
    mock.get_metadata.return_value = resources_pb2.FieldConversation(pages=1, total=5)
    mock.db_get_value.return_value = resources_pb2.Conversation(messages=messages)

    yield mock


@pytest.fixture()
def kb(field_obj):
    mock = AsyncMock()
    mock.get.return_value.get_field.return_value = field_obj
    yield mock


async def test_get_next_conversation_messages(field_obj, messages):
    assert (
        len(
            await chat_prompt.get_next_conversation_messages(
                field_obj=field_obj, page=1, start_idx=0, num_messages=5
            )
        )
        == 5
    )
    assert (
        len(
            await chat_prompt.get_next_conversation_messages(
                field_obj=field_obj, page=1, start_idx=0, num_messages=1
            )
        )
        == 1
    )

    assert await chat_prompt.get_next_conversation_messages(
        field_obj=field_obj,
        page=1,
        start_idx=0,
        num_messages=1,
        message_type=resources_pb2.Message.MessageType.ANSWER,
        msg_to="1",
    ) == [messages[3]]


async def test_find_conversation_message(field_obj, messages):
    assert await chat_prompt.find_conversation_message(
        field_obj=field_obj, mident="3"
    ) == (messages[2], 1, 2)


async def test_get_expanded_conversation_messages(kb, messages):
    assert await chat_prompt.get_expanded_conversation_messages(
        kb=kb, rid="rid", field_id="field_id", mident="3"
    ) == [messages[3]]


async def test_get_expanded_conversation_messages_question(kb, messages):
    assert (
        await chat_prompt.get_expanded_conversation_messages(
            kb=kb, rid="rid", field_id="field_id", mident="1"
        )
        == messages[1:]
    )

    kb.get.assert_called_with("rid")
    kb.get.return_value.get_field.assert_called_with(
        "field_id", KB_REVERSE["c"], load=True
    )


async def test_get_expanded_conversation_messages_missing(kb, messages):
    assert (
        await chat_prompt.get_expanded_conversation_messages(
            kb=kb, rid="rid", field_id="field_id", mident="missing"
        )
        == []
    )


def _create_find_result(
    _id: str, result_text: str, score_type: SCORE_TYPE = SCORE_TYPE.BM25
):
    return FindResource(
        id=_id.split("/")[0],
        fields={
            "c/conv": FindField(
                paragraphs={
                    _id: FindParagraph(
                        id=_id,
                        score=1.0,
                        score_type=score_type,
                        order=1,
                        text=result_text,
                    )
                }
            )
        },
    )


async def test_format_chat_prompt_content(kb):
    result_text = " ".join(["text"] * 10)
    with patch("nucliadb.search.search.chat.prompt.get_driver"), patch(
        "nucliadb.search.search.chat.prompt.get_storage"
    ), patch("nucliadb.search.search.chat.prompt.KnowledgeBoxORM", return_value=kb):
        prompt_result = await chat_prompt.format_chat_prompt_content(
            "kbid",
            KnowledgeboxFindResults(
                facets={},
                resources={
                    "bmid": _create_find_result(
                        "bmid/c/conv/ident", result_text, SCORE_TYPE.BM25
                    ),
                    "vecid": _create_find_result(
                        "vecid/c/conv/ident", result_text, SCORE_TYPE.VECTOR
                    ),
                },
                min_score=-1,
            ),
        )
        assert prompt_result == chat_prompt.PROMPT_TEXT_RESULT_SEP.join(
            [result_text, result_text]
        )
