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
from datetime import datetime

import pytest
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
)
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.reader.api.models import ResourceField
from nucliadb.tests.utils import inject_message
from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
    MessageType,
)
from nucliadb_models.resource import ConversationFieldData, FieldConversation
from nucliadb_models.resource import Resource
from nucliadb_models.resource import Resource as ResponseResponse
from nucliadb_models.search import KnowledgeboxFindResults
from nucliadb_models.writer import CreateResourcePayload


@pytest.fixture(scope="function")
async def resource_with_conversation(nucliadb_grpc, nucliadb_writer, knowledgebox):
    messages = []
    for i in range(300):
        messages.append(
            InputMessage(
                to=["computer"],
                who=f"person{i}",
                timestamp=datetime.now(),
                content=InputMessageContent(text="What is the meaning of life?"),
                ident=str(i),
                type=MessageType.QUESTION.value,
            )
        )
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        headers={"Content-Type": "application/json"},
        data=CreateResourcePayload(  # type: ignore
            slug="myresource",
            conversations={
                "faq": InputConversationField(messages=messages),
            },
        ).json(by_alias=True),
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # add another message using the api to add single message
    resp = await nucliadb_writer.put(
        f"/kb/{knowledgebox}/resource/{rid}/conversation/faq/messages",
        data="["  # type: ignore
        + InputMessage(
            to=[f"computer"],
            content=InputMessageContent(text="42"),
            ident="computer",
            type=MessageType.ANSWER.value,
        ).json(by_alias=True)
        + "]",
    )

    assert resp.status_code == 200

    # Inject synthetic extracted data for the conversation
    extracted_split_text = {"1": "Split text 1", "2": "Split text 2"}

    bm = BrokerMessage()
    bm.uuid = rid
    bm.kbid = knowledgebox
    field = FieldID(field="faq", field_type=FieldType.CONVERSATION)

    etw = ExtractedTextWrapper()
    etw.field.MergeFrom(field)
    etw.body.text = ""  # with convos, text is empty
    etw.body.split_text.update(extracted_split_text)
    bm.extracted_text.append(etw)

    fmw = FieldComputedMetadataWrapper()
    fmw.field.MergeFrom(field)
    for split, text in extracted_split_text.items():
        paragraph = Paragraph(start=0, end=len(text), kind=Paragraph.TypeParagraph.TEXT)
        fmw.metadata.split_metadata[split].paragraphs.append(paragraph)
    bm.field_metadata.append(fmw)

    await inject_message(nucliadb_grpc, bm)

    yield rid


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_conversations(
    nucliadb_reader: AsyncClient,
    knowledgebox,
    resource_with_conversation,
):
    rid = resource_with_conversation

    # get field summary
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}?show=values")
    assert resp.status_code == 200

    res_resp = ResponseResponse.parse_obj(resp.json())

    assert res_resp.data.conversations["faq"] == ConversationFieldData(  # type: ignore
        value=FieldConversation(pages=2, size=200, total=301),
        extracted=None,
        error=None,
    )

    # get first page
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}/conversation/faq?page=1"
    )
    assert resp.status_code == 200
    field_resp = ResourceField.parse_obj(resp.json())
    msgs = field_resp.value["messages"]  # type: ignore
    assert len(msgs) == 200
    assert [m["ident"] for m in msgs] == [str(i) for i in range(200)]
    assert msgs[0]["type"] == MessageType.QUESTION.value

    # get second page
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}/conversation/faq?page=2"
    )
    assert resp.status_code == 200
    field_resp = ResourceField.parse_obj(resp.json())
    msgs = field_resp.value["messages"]  # type: ignore
    assert len(msgs) == 101
    assert [m["ident"] for m in msgs] == [str(i) for i in range(200, 300)] + [
        "computer"
    ]
    assert msgs[-1]["type"] == MessageType.ANSWER.value


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_extracted_text_is_serialized_properly(
    nucliadb_reader: AsyncClient,
    knowledgebox,
    resource_with_conversation,
):
    rid = resource_with_conversation

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}?show=values&show=extracted&extracted=text",
    )
    assert resp.status_code == 200
    resource = Resource.parse_obj(resp.json())
    extracted = resource.data.conversations["faq"].extracted  # type: ignore
    assert extracted.text.text == ""  # type: ignore
    assert extracted.text.split_text["1"] == "Split text 1"  # type: ignore
    assert extracted.text.split_text["2"] == "Split text 2"  # type: ignore


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_find_conversations(
    nucliadb_reader: AsyncClient,
    knowledgebox,
    resource_with_conversation,
):
    rid = resource_with_conversation

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/find?query=&show=values&show=extracted&extracted=text",
    )
    assert resp.status_code == 200
    results = KnowledgeboxFindResults.parse_obj(resp.json())
    resource = results.resources[rid]

    # Check extracted
    conversation = resource.data.conversations["faq"]  # type: ignore
    extracted = conversation.extracted
    assert extracted.text.text == ""  # type: ignore
    assert extracted.text.split_text["1"] == "Split text 1"  # type: ignore
    assert extracted.text.split_text["2"] == "Split text 2"  # type: ignore

    # Check paragraph positions match the split text values
    field = resource.fields["/c/faq"]
    paragraphs = field.paragraphs
    assert len(paragraphs) == 2
    assert paragraphs[f"{rid}/c/faq/1/0-12"].text == "Split text 1"
    assert paragraphs[f"{rid}/c/faq/2/0-12"].text == "Split text 2"
