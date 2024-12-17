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
from unittest import mock

import pytest

from nucliadb.ingest.fields.conversation import Conversation
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import Message as PBMessage
from nucliadb_protos.resources_pb2 import MessageContent as PBMessageContent


class MockTransaction:
    def __init__(self, *args, **kwargs):
        self.data = {}

    async def set(self, key, value):
        self.data[key] = value

    async def get(self, key):
        return self.data.get(key, None)


@pytest.fixture(scope="function")
def txn():
    txn = MockTransaction()
    yield txn


@pytest.fixture(scope="function")
def storage():
    storage = mock.AsyncMock()
    storage.needs_move = mock.Mock(return_value=False)
    yield storage


@pytest.fixture(scope="function")
def resource(txn, storage):
    resource = mock.AsyncMock()
    resource.modified = False
    resource.txn = txn
    resource.storage = storage
    yield resource


async def test_get_metadata(resource):
    conv = Conversation("faq", resource)
    assert conv.value == {}
    assert conv.metadata is None
    assert conv._created is False

    metadata = await conv.get_metadata()
    assert conv._created is True
    assert metadata.size == 200
    assert metadata.pages == 0
    assert metadata.total == 0

    metadata.pages = 1
    metadata.total = 10

    await conv.db_set_metadata(metadata)
    assert conv.metadata == metadata
    assert conv.resource.modified is True
    assert conv._created is False

    # Check it is cached
    assert await conv.get_metadata() == metadata

    # Force reload
    conv.metadata = None
    assert await conv.get_metadata() == metadata
    assert conv._created is False


def get_cf(uri):
    cf = CloudFile(uri=uri)
    cf.filename = "foo.txt"
    cf.size = 100
    cf.content_type = "text/plain"
    cf.bucket_name = "bucket"
    return cf


def get_message(id, who, to, text, attachments=None):
    msg = PBMessage(ident=id, who=who)

    if not isinstance(to, list):
        to = [to]

    msg.to.extend(to)
    msg.timestamp.GetCurrentTime()
    msg.content.text = text
    msg.content.format = PBMessageContent.Format.PLAIN
    for attachment in attachments or []:
        msg.content.attachments.append(attachment)

    return msg


async def test_get_value(resource):
    conv = Conversation("faq", resource)

    cf = get_cf("gs://bucket/foo.txt")
    msg1 = get_message("m1", "person", "computer", "What is the meaning of life?", [cf])
    msg2 = get_message("m2", "computer", "person", "42")

    payload = PBConversation()
    payload.messages.extend([msg1, msg2])

    await conv.set_value(payload)

    metadata = await conv.get_metadata()
    assert metadata.size == 200
    assert metadata.pages == 1
    assert metadata.total == 2

    payload = PBConversation()
    for i in range(300):
        msg = get_message(str(i), "person", "computer", "What is the meaning of life?")
        payload.messages.append(msg)

    await conv.set_value(payload)

    conv.metadata = None
    metadata = await conv.get_metadata()
    assert metadata.size == 200
    assert metadata.pages == 2
    assert metadata.total == 302

    # Pages start at 1
    assert await conv.get_value() is None
    assert await conv.get_value(page=0) is None

    for force_reload in [True, False]:
        if force_reload:
            conv.metadata = None
            conv.value = {}

        page1 = await conv.get_value(page=1)
        assert len(page1.messages) == 200
        assert [m.ident for m in page1.messages] == ["m1", "m2"] + [str(i) for i in range(198)]

        page2 = await conv.get_value(page=2)
        assert len(page2.messages) == 102
        assert [m.ident for m in page2.messages] == [str(i) for i in range(198, 300)]
