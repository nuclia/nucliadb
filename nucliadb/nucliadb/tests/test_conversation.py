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
import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
)
from nucliadb_models.resource import (
    Resource as ResponseResponse,
    ConversationFieldData,
    FieldConversation,
)


@pytest.mark.asyncio
async def test_conversations(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        headers={"Content-Type": "application/json"},
        data=CreateResourcePayload(
            slug="myresource",
            conversations={
                "faq": InputConversationField(
                    messages=[
                        InputMessage(
                            to=["computer"],
                            content=InputMessageContent(
                                text="What is the meaning of life?"
                            ),
                            ident="1",
                        ),
                        InputMessage(
                            to=["humans"],
                            content=InputMessageContent(text="42"),
                            ident="2",
                        ),
                    ]
                ),
            },
        ).json(),
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}?show=values")
    assert resp.status_code == 200
    res_resp = ResponseResponse.parse_obj(resp.json())

    assert res_resp.data.conversations["faq"] == ConversationFieldData(
        value=FieldConversation(pages=1, size=200), extracted=None, error=None
    )

    # XXX Currently no way to get conversation pages through API
