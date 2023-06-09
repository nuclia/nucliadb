from unittest.mock import AsyncMock

import pytest

from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.search.search import chat_prompt
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
