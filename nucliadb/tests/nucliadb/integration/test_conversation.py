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

from nucliadb.reader.api.models import ResourceField
from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
    MessageType,
)
from nucliadb_models.resource import ConversationFieldData, FieldConversation, Resource
from nucliadb_models.resource import Resource as ResponseResponse
from nucliadb_models.search import KnowledgeboxFindResults
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
)
from nucliadb_protos.writer_pb2 import BrokerMessage
from tests.utils import inject_message


@pytest.fixture(scope="function")
async def resource_with_conversation(
    nucliadb_ingest_grpc, nucliadb_writer: AsyncClient, standalone_knowledgebox
):
    messages = []
    for i in range(1, 301):
        messages.append(
            InputMessage(
                to=["computer"],
                who=f"person{i}",
                timestamp=datetime.now(),
                content=InputMessageContent(text="What is the meaning of life?"),
                ident=str(i),
                type=MessageType.QUESTION,
            )
        )
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        headers={"Content-Type": "application/json"},
        content=CreateResourcePayload(
            slug="myresource",
            conversations={
                "faq": InputConversationField(messages=messages),
            },
        ).model_dump_json(by_alias=True),
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # add another message using the api to add single message
    resp = await nucliadb_writer.put(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/conversation/faq/messages",
        content="["
        + InputMessage(
            to=[f"computer"],
            content=InputMessageContent(text="42"),
            ident="computer",
            type=MessageType.ANSWER,
        ).model_dump_json(by_alias=True)
        + "]",
    )

    assert resp.status_code == 200

    # Inject synthetic extracted data for the conversation
    extracted_split_text = {"1": "Split text 1", "2": "Split text 2"}

    bm = BrokerMessage()
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.uuid = rid
    bm.kbid = standalone_knowledgebox
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

    await inject_message(nucliadb_ingest_grpc, bm)

    yield rid


@pytest.mark.deploy_modes("standalone")
async def test_conversations(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
    resource_with_conversation,
):
    rid = resource_with_conversation

    # get field summary
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/resource/{rid}?show=values")
    assert resp.status_code == 200

    res_resp = ResponseResponse.model_validate(resp.json())

    assert res_resp.data.conversations["faq"] == ConversationFieldData(  # type: ignore
        value=FieldConversation(pages=2, size=200, total=301, extract_strategy=""),
        extracted=None,
        error=None,
    )

    # get first page
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/conversation/faq?page=1"
    )
    assert resp.status_code == 200
    field_resp = ResourceField.model_validate(resp.json())
    msgs = field_resp.value["messages"]  # type: ignore
    assert len(msgs) == 200
    assert [m["ident"] for m in msgs] == [str(i) for i in range(1, 201)]
    assert msgs[0]["type"] == MessageType.QUESTION.value

    # get second page
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/conversation/faq?page=2"
    )
    assert resp.status_code == 200
    field_resp = ResourceField.model_validate(resp.json())
    msgs = field_resp.value["messages"]  # type: ignore
    assert len(msgs) == 101
    assert [m["ident"] for m in msgs] == [str(i) for i in range(201, 301)] + ["computer"]
    assert msgs[-1]["type"] == MessageType.ANSWER.value


@pytest.mark.deploy_modes("standalone")
async def test_extracted_text_is_serialized_properly(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
    resource_with_conversation,
):
    rid = resource_with_conversation

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=values&show=extracted&extracted=text",
    )
    assert resp.status_code == 200
    resource = Resource.model_validate(resp.json())
    extracted = resource.data.conversations["faq"].extracted  # type: ignore
    assert extracted.text.text == ""  # type: ignore
    assert extracted.text.split_text["1"] == "Split text 1"  # type: ignore
    assert extracted.text.split_text["2"] == "Split text 2"  # type: ignore


@pytest.mark.deploy_modes("standalone")
async def test_find_conversations(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_conversation,
):
    rid = resource_with_conversation

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/find?query=",
    )
    assert resp.status_code == 200
    results = KnowledgeboxFindResults.model_validate(resp.json())
    matching_rid, matching_resource = results.resources.popitem()
    assert matching_rid == rid

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{rid}?show=values&show=extracted&extracted=text"
    )
    assert resp.status_code == 200
    resource = Resource.model_validate(resp.json())

    # Check extracted
    conversation = resource.data.conversations["faq"]  # type: ignore
    extracted = conversation.extracted
    assert extracted.text.text == ""  # type: ignore
    assert extracted.text.split_text["1"] == "Split text 1"  # type: ignore
    assert extracted.text.split_text["2"] == "Split text 2"  # type: ignore

    # Check paragraph positions match the split text values
    field = matching_resource.fields["/c/faq"]
    paragraphs = field.paragraphs
    assert len(paragraphs) == 2
    assert paragraphs[f"{rid}/c/faq/1/0-12"].text == "Split text 1"
    assert paragraphs[f"{rid}/c/faq/2/0-12"].text == "Split text 2"


@pytest.mark.deploy_modes("standalone")
async def test_cannot_create_message_ident_0(
    nucliadb_ingest_grpc, nucliadb_writer: AsyncClient, standalone_knowledgebox: str
):
    messages = [
        # model_construct skips validation, to test the API error
        InputMessage.model_construct(
            to=["computer"],
            who=f"person",
            timestamp=datetime.now(),
            content=InputMessageContent(text="What is the meaning of life?"),
            ident="0",
            type=MessageType.QUESTION,
        )
    ]
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        headers={"Content-Type": "application/json"},
        content=CreateResourcePayload(
            slug="myresource",
            conversations={
                "faq": InputConversationField(messages=messages),
            },
        ).model_dump_json(by_alias=True),
    )

    assert resp.status_code == 422
    assert 'cannot be "0"' in resp.json()["detail"][0]["msg"]
