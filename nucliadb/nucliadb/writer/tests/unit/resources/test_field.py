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
from nucliadb_models.common import FileB64
from nucliadb_models.conversation import InputConversationField, InputMessageContent, InputMessage

import pytest
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest.processing import PushPayload
from nucliadb.writer.resource.field import parse_conversation_field, parse_file_field
from nucliadb_models import File, FileField

FIELD_MODULE = "nucliadb.writer.resource.field"


@pytest.fixture(scope="function")
def processing_mock():
    with mock.patch(f"{FIELD_MODULE}.get_processing") as get_processing_mock:
        processing = mock.Mock()
        processing.convert_filefield_to_str = mock.AsyncMock(return_value="internal")
        processing.convert_external_filefield_to_str.return_value = "external"
        get_processing_mock.return_value = processing
        yield processing


@pytest.fixture(scope="function")
def storage_mock():
    cf = CloudFile()
    storage = mock.AsyncMock()
    storage.upload_b64file_to_cloudfile = mock.AsyncMock(return_value=cf)
    with mock.patch(f"{FIELD_MODULE}.get_storage", return_value=storage):
        yield storage


@pytest.mark.parametrize(
    "file_field",
    [
        FileField(password="mypassword", file=File(filename="myfile.pdf", payload="")),
        FileField(
            password="mypassword", file=File(uri="http://external.foo/myfile.pdf")
        ),
    ],
)
@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_parse_conversation_field(storage_mock, processing_mock):
    key = "conv"
    kbid = "kbid"
    uuid = "uuid"
    bm = BrokerMessage()
    pp = PushPayload(uuid=uuid, kbid=kbid, partition=1, userid="user")
    attachment = get_file()
    m1 = get_message(id="m1", who="ramon", to="nucliadb", text="look at this idea", attachments=[attachment])
    m2 = get_message(id="m2", who="nucliadb", to="ramon", text="ok")

    conversation_field = InputConversationField(messages=[m1, m2])

    await parse_conversation_field(key, conversation_field, bm, pp, kbid, uuid)

    breakpoint()
    pass
