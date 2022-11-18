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

from datetime import datetime
from uuid import uuid4

from nucliadb_client.knowledgebox import KnowledgeBox
from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
    MessageFormat,
)
from nucliadb_models.datetime import FieldDatetime
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdString
from nucliadb_models.writer import CreateResourcePayload


def test_resource_creation(nucliadb_knowledgebox: KnowledgeBox):
    payload = CreateResourcePayload()
    payload.icon = "plain/text"
    payload.title = "My Resource"
    payload.summary = "My long summary of the resource"
    payload.slug = "myresource"  # type: ignore

    payload.conversations[FieldIdString("conv1")] = InputConversationField(
        message=[
            InputMessage(
                timestamp=datetime.now(),
                who="myself",
                to=["you", "he"],
                content=InputMessageContent(
                    text="Hello how are you ?", format=MessageFormat.PLAIN
                ),
                ident=uuid4().hex,
            ),
            InputMessage(
                timestamp=datetime.now(),
                who="you",
                to=["myself", "he"],
                content=InputMessageContent(
                    text="Superlekker!", format=MessageFormat.PLAIN
                ),
                ident=uuid4().hex,
            ),
        ]
    )

    payload.texts[FieldIdString("text1")] = TextField(body="My lovely text")
    payload.texts[FieldIdString("text2")] = TextField(body="My second lovely text")

    payload.datetimes[FieldIdString("date1")] = FieldDatetime(value=datetime.now())
    payload.datetimes[FieldIdString("date2")] = FieldDatetime(value=datetime.now())

    res = nucliadb_knowledgebox.create_resource(payload)

    info = res.get()
    assert info.title == "My Resource"


def test_reprocess(nucliadb_knowledgebox: KnowledgeBox):
    payload = CreateResourcePayload()
    payload.title = "My Resource"
    payload.icon = "plain/text"
    res = nucliadb_knowledgebox.create_resource(payload)

    res.reprocess()


def test_reindex(nucliadb_knowledgebox: KnowledgeBox):
    payload = CreateResourcePayload()
    payload.title = "My Resource"
    payload.icon = "plain/text"
    res = nucliadb_knowledgebox.create_resource(payload)

    res.reindex()
    res.reindex(vectors=True)
