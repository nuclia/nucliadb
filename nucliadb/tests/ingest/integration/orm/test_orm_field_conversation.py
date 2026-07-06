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
from os.path import dirname, getsize
from uuid import uuid4

from nucliadb.ingest.fields.conversation import PAGE_SIZE, Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_protos.resources_pb2 import CloudFile, FieldType, Message, MessageContent
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_utils.storages.storage import Storage


async def test_create_resource_orm_field_conversation(
    storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    c2 = PBConversation()
    conv1 = Message(
        who="myself",
    )
    conv1.timestamp.FromDatetime(datetime.now())
    conv1.content.text = "hello"
    conv1.content.format = MessageContent.Format.PLAIN
    c2.messages.append(conv1)
    await r.set_field(FieldType.CONVERSATION, "conv1", c2)

    convfield: Conversation = await r.get_field("conv1", FieldType.CONVERSATION, load=True)
    assert convfield.metadata is not None
    assert convfield.metadata.pages == 1
    assert convfield.value[1].messages[0].content.text == "hello"

    c2.ClearField("messages")
    for i in range(300):
        conv1.content.text = f"{i} hello"
        c2.messages.append(conv1)
    await r.set_field(FieldType.CONVERSATION, "conv1", c2)

    convfield = await r.get_field("conv1", FieldType.CONVERSATION, load=True)
    assert convfield.metadata is not None
    assert convfield.metadata.pages == 2
    assert convfield.value[1].messages[0].content.text == "hello"
    assert convfield.value[2].messages[0].content.text == "199 hello"
    assert len(convfield.value[1].messages) == PAGE_SIZE
    assert len(convfield.value[2].messages) == 301 - PAGE_SIZE
    assert convfield.value[1].messages[-1].content.text == "198 hello"
    assert convfield.value[2].messages[-1].content.text == "299 hello"


async def test_create_resource_orm_field_conversation_file(
    local_files, storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None

    c2 = PBConversation()
    conv1 = Message(
        who="myself",
    )
    conv1.timestamp.FromDatetime(datetime.now())
    conv1.content.text = "hello"
    conv1.content.format = MessageContent.Format.PLAIN

    filename = f"{dirname(__file__)}/assets/file.png"

    cf1 = CloudFile(
        uri="file.png",
        source=CloudFile.Source.LOCAL,
        bucket_name="/integration/orm/assets",
        size=getsize(filename),
        content_type="image/png",
        filename="file.png",
    )
    conv1.content.attachments.append(cf1)
    c2.messages.append(conv1)

    await r.set_field(FieldType.CONVERSATION, "conv1", c2)

    convfield: Conversation = await r.get_field("conv1", FieldType.CONVERSATION, load=True)
    assert convfield.metadata is not None
    assert convfield.metadata.pages == 1
    assert convfield.value[1].messages[0].content.text == "hello"

    assert convfield.value[1].messages[0].content.attachments[0].source == storage.source
    data = await storage.downloadbytescf(convfield.value[1].messages[0].content.attachments[0])

    with open(filename, "rb") as testfile:
        data2 = testfile.read()
    assert data.read() == data2
