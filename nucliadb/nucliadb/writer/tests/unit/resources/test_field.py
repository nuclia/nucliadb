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
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest.processing import PushPayload
from nucliadb.writer.resource.field import parse_file_field
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
