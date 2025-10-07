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

from nucliadb.models.internal.processing import PushMessageFormat, PushPayload
from nucliadb.writer.resource.field import (
    ResourceClassifications,
    parse_conversation_field,
    parse_file_field,
)
from nucliadb_models import File, FileField
from nucliadb_models.common import FileB64
from nucliadb_models.conversation import (
    InputConversationField,
    InputMessage,
    InputMessageContent,
)
from nucliadb_protos.resources_pb2 import CloudFile, MessageContent
from nucliadb_protos.writer_pb2 import BrokerMessage

FIELD_MODULE = "nucliadb.writer.resource.field"


@pytest.fixture(scope="function")
def processing_mock():
    processing = mock.Mock()
    processing.convert_filefield_to_str = mock.AsyncMock(return_value="internal")
    processing.convert_external_filefield_to_str.return_value = "external"
    processing.convert_internal_cf_to_str = mock.AsyncMock(return_value="internal")
    with mock.patch(f"{FIELD_MODULE}.get_processing", return_value=processing):
        yield processing


@pytest.fixture(scope="function")
def storage_mock():
    cf = CloudFile()
    cf.bucket_name = "bucket"
    cf.uri = "fullkey"
    cf.filename = "payload.pb"

    storage = mock.AsyncMock()
    storage.upload_b64file_to_cloudfile = mock.AsyncMock(return_value=cf)

    with mock.patch(f"{FIELD_MODULE}.get_storage", return_value=storage):
        yield storage


@pytest.mark.parametrize(
    "file_field",
    [
        FileField(password="mypassword", file=File(filename="myfile.pdf", payload="")),
        FileField(
            password="mypassword",
            file=File(uri="http://external.foo/myfile.pdf"),  # type: ignore
        ),
    ],
)
async def test_parse_file_field_does_not_store_password(processing_mock, file_field):
    field_key = "key"
    kbid = "kbid"
    uuid = "uuid"
    writer_bm = BrokerMessage()

    await parse_file_field(
        field_key,
        file_field,
        writer_bm,
        PushPayload(kbid=kbid, uuid=uuid, partition=1, userid="user"),
        kbid,
        uuid,
        ResourceClassifications(),
        skip_store=True,
    )

    assert not writer_bm.files[field_key].password


def get_file():
    return FileB64(filename="myfile.pdf", payload="foo", md5="bar")


def get_message(id, who, to, text, attachments=None):
    content = InputMessageContent(text=text)
    if not isinstance(to, list):
        to = [to]
    if attachments:
        content.attachments.extend(attachments)
    msg = InputMessage(who=who, to=to, content=content, ident=id)
    return msg


@pytest.fixture()
def conv_messages_count():
    with mock.patch(f"{FIELD_MODULE}.get_current_conversation_message_count", return_value=0):
        yield


async def test_parse_conversation_field(storage_mock, processing_mock, conv_messages_count):
    key = "conv"
    kbid = "kbid"
    uuid = "uuid"
    bm = BrokerMessage()
    pp = PushPayload(uuid=uuid, kbid=kbid, partition=1, userid="user")
    attachment = get_file()
    m1 = get_message(
        id="m1",
        who="ramon",
        to="nucliadb",
        text="look at this idea",
        attachments=[attachment],
    )
    m2 = get_message(id="m2", who="nucliadb", to="ramon", text="ok")

    conversation_field = InputConversationField(messages=[m1, m2])

    await parse_conversation_field(
        key, conversation_field, bm, pp, kbid, uuid, ResourceClassifications()
    )

    # Check push payload
    assert len(pp.conversationfield[key].messages) == 2
    ppm1 = pp.conversationfield[key].messages[0]
    assert ppm1.ident == "m1"
    assert ppm1.who == "ramon"
    assert ppm1.to == ["nucliadb"]
    assert ppm1.content.text == "look at this idea"
    assert ppm1.content.format == PushMessageFormat.PLAIN
    assert ppm1.content.attachments[0] == "internal"
    ppm2 = pp.conversationfield[key].messages[1]
    assert ppm2.ident == "m2"
    assert ppm2.who == "nucliadb"
    assert ppm2.to == ["ramon"]
    assert ppm2.content.text == "ok"
    assert ppm2.content.format == PushMessageFormat.PLAIN
    assert ppm2.content.attachments == []

    # Check broker message
    bmm1 = bm.conversations[key].messages[0]
    assert bmm1.ident == "m1"
    assert bmm1.who == "ramon"
    assert bmm1.to == ["nucliadb"]
    assert bmm1.content.text == "look at this idea"
    assert bmm1.content.format == MessageContent.Format.PLAIN
    assert len(bmm1.content.attachments) == 1
    cf = bmm1.content.attachments[0]
    assert cf.filename == "payload.pb"
    assert cf.bucket_name == "bucket"
    assert cf.uri == "fullkey"
    bmm2 = bm.conversations[key].messages[1]
    assert bmm2.ident == "m2"
    assert bmm2.who == "nucliadb"
    assert bmm2.to == ["ramon"]
    assert bmm2.content.text == "ok"
    assert bmm2.content.format == MessageContent.Format.PLAIN
    assert len(bmm2.content.attachments) == 0

    # Check storage calls
    assert storage_mock.conversation_field_attachment.call_count == 1
    assert storage_mock.upload_b64file_to_cloudfile.await_count == 1

    # Check processing calls
    assert processing_mock.convert_internal_cf_to_str.await_count == 1
