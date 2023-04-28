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
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.reader.api.models import ResourceField
from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
)
from nucliadb_models.resource import ConversationFieldData, FieldConversation
from nucliadb_models.resource import Resource as ResponseResponse
from nucliadb_models.writer import CreateResourcePayload


@pytest.mark.asyncio
async def test_conversations(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    messages = []
    for i in range(300):
        messages.append(
            InputMessage(
                to=["computer"],
                who=f"person{i}",
                timestamp=datetime.now(),
                content=InputMessageContent(text="What is the meaning of life?"),
                ident=str(i),
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
        ).json(),
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
        ).json()
        + "]",
    )

    assert resp.status_code == 200

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
    assert len(field_resp.value["messages"]) == 200  # type: ignore

    # get second page
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}/conversation/faq?page=2"
    )
    assert resp.status_code == 200
    field_resp = ResourceField.parse_obj(resp.json())
    assert len(field_resp.value["messages"]) == 101  # type: ignore
