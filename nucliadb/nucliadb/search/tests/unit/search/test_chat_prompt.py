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
from unittest import mock
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
    MinScore,
)
from nucliadb_protos import resources_pb2


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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_find_conversation_message(field_obj, messages):
    assert await chat_prompt.find_conversation_message(
        field_obj=field_obj, mident="3"
    ) == (messages[2], 1, 2)


@pytest.mark.asyncio
async def test_get_expanded_conversation_messages(kb, messages):
    assert await chat_prompt.get_expanded_conversation_messages(
        kb=kb, rid="rid", field_id="field_id", mident="3"
    ) == [messages[3]]


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_get_expanded_conversation_messages_missing(kb, messages):
    assert (
        await chat_prompt.get_expanded_conversation_messages(
            kb=kb, rid="rid", field_id="field_id", mident="missing"
        )
        == []
    )


def _create_find_result(
    _id: str, result_text: str, score_type: SCORE_TYPE = SCORE_TYPE.BM25, order=1
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
                        order=order,
                        text=result_text,
                    )
                }
            )
        },
    )


@pytest.mark.asyncio
async def test_default_prompt_context(kb):
    result_text = " ".join(["text"] * 10)
    with patch("nucliadb.search.search.chat.prompt.get_read_only_transaction"), patch(
        "nucliadb.search.search.chat.prompt.get_storage"
    ), patch("nucliadb.search.search.chat.prompt.KnowledgeBoxORM", return_value=kb):
        context = chat_prompt.CappedPromptContext(max_size=1e6)
        await chat_prompt.default_prompt_context(
            context,
            "kbid",
            KnowledgeboxFindResults(
                facets={},
                resources={
                    "bmid": _create_find_result(
                        "bmid/c/conv/ident", result_text, SCORE_TYPE.BM25, order=1
                    ),
                    "vecid": _create_find_result(
                        "vecid/c/conv/ident", result_text, SCORE_TYPE.VECTOR, order=2
                    ),
                    "both_id": _create_find_result(
                        "both_id/c/conv/ident", result_text, SCORE_TYPE.BOTH, order=0
                    ),
                },
                min_score=MinScore(semantic=-1),
            ),
        )
        prompt_result = context.output
        # Check that the results are sorted by increasing order and that the extra
        # context is added at the beginning, indicating that it has the most priority
        paragraph_ids = [pid for pid in prompt_result.keys()]
        assert paragraph_ids == [
            "both_id/c/conv/ident",
            "bmid/c/conv/ident",
            "vecid/c/conv/ident",
        ]


@pytest.fixture(scope="function")
def find_results():
    return KnowledgeboxFindResults(
        facets={},
        resources={
            "resource1": _create_find_result(
                "resource1/a/title", "Resource 1", SCORE_TYPE.BOTH, order=1
            ),
            "resource2": _create_find_result(
                "resource2/a/title", "Resource 2", SCORE_TYPE.VECTOR, order=2
            ),
        },
        min_score=MinScore(semantic=-1),
    )


@pytest.mark.asyncio
async def test_prompt_context_builder_prepends_user_context(
    find_results: KnowledgeboxFindResults,
):
    builder = chat_prompt.PromptContextBuilder(
        kbid="kbid", find_results=find_results, user_context=["Carrots are orange"]
    )

    async def _mock_build_context(context, *args, **kwargs):
        context["resource1/a/title"] = "Resource 1"
        context["resource2/a/title"] = "Resource 2"

    with mock.patch.object(builder, "_build_context", new=_mock_build_context):
        context, context_order = await builder.build()
        assert len(context) == 3
        assert len(context_order) == 3
        assert context["USER_CONTEXT_0"] == "Carrots are orange"
        assert context["resource1/a/title"] == "Resource 1"
        assert context["resource2/a/title"] == "Resource 2"
        assert context_order["USER_CONTEXT_0"] == 0
        assert context_order["resource1/a/title"] == 1
        assert context_order["resource2/a/title"] == 2


def test_capped_prompt_context():
    context = chat_prompt.CappedPromptContext(max_size=2)

    # Check that the exception is raised
    with pytest.raises(chat_prompt.MaxContextSizeExceeded):
        context["key1"] = "123"

    assert context.output == {}
    assert context.size == 0

    # Add a new value
    context["key1"] = "f"
    assert context.output == {"key1": "f"}
    assert context.size == 1

    # Update existing value
    context["key1"] = "fo"
    assert context.output == {"key1": "fo"}
    assert context.size == 2

    # It should not accept new values now
    with pytest.raises(chat_prompt.MaxContextSizeExceeded):
        context["key1"] = "foo"
    with pytest.raises(chat_prompt.MaxContextSizeExceeded):
        context["key2"] = "f"

    # Check without limits
    context = chat_prompt.CappedPromptContext(max_size=None)
    context["key1"] = "foo" * int(1e6)
