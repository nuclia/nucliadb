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
from unittest.mock import AsyncMock, Mock, patch

import pytest
from fastapi.requests import Request

from nucliadb.ingest.processing import ProcessingInfo, Source
from nucliadb.writer.api.v1.upload import (
    guess_content_type,
    store_file_on_nuclia_db,
    validate_field_upload,
)
from nucliadb.writer.tus.exceptions import HTTPConflict, HTTPNotFound
from nucliadb_models.resource import QueueType

UPLOAD_PACKAGE = "nucliadb.writer.api.v1.upload"


@pytest.fixture(scope="function")
def partitioning_mock():
    with patch(f"{UPLOAD_PACKAGE}.get_partitioning"):
        yield


@pytest.fixture(scope="function")
def processing_mock():
    with patch(f"{UPLOAD_PACKAGE}.get_processing") as get_processing_mock:
        processing = AsyncMock()
        processing.convert_internal_filefield_to_str.return_value = "foo"
        processing_info = ProcessingInfo(seqid=1, account_seq=1, queue=QueueType.SHARED)
        processing.send_to_process.return_value = processing_info
        get_processing_mock.return_value = processing
        yield processing


@pytest.fixture(scope="function")
def transaction_mock():
    with patch(f"{UPLOAD_PACKAGE}.get_transaction_utility") as transaction_mock:
        transaction = AsyncMock()
        transaction_mock.return_value = transaction
        yield transaction


@pytest.mark.asyncio
async def test_store_file_on_nucliadb_does_not_store_passwords(
    processing_mock, partitioning_mock, transaction_mock
):
    field = "myfield"

    await store_file_on_nuclia_db(
        10,
        "kbid",
        "/some/path",
        Request({"type": "http", "headers": []}),
        "bucket",
        Source.INGEST,
        "rid",
        field,
        password="mypassword",
    )

    transaction_mock.commit.assert_awaited_once()
    writer_bm = transaction_mock.commit.call_args[0][0]
    assert not writer_bm.files[field].password


@pytest.mark.parametrize(
    "filename,content_type",
    [
        ("foo.png", "image/png"),
        ("foo.pdf", "application/pdf"),
        ("someuuidwithoutextension", "application/octet-stream"),
        ("", "application/octet-stream"),
    ],
)
def test_guess_content_type(filename, content_type):
    assert guess_content_type(filename) == content_type


@pytest.mark.parametrize(
    "rid,field,md5,exists,result",
    [
        (None, None, None, False, ("uuid4", "uuid4")),
        (None, None, None, True, ("uuid4", "uuid4")),
        (None, None, "md5", False, ("md5", "md5")),
        (None, None, "md5", True, HTTPConflict),
        (None, "field", None, False, ("uuid4", "field")),
        (None, "field", None, True, ("uuid4", "field")),
        (None, "field", "md5", False, ("md5", "field")),
        (None, "field", "md5", True, HTTPConflict),
        ("rid", None, None, False, HTTPNotFound),
        ("rid", None, None, True, ("rid", "uuid4")),
        ("rid", None, "md5", False, HTTPNotFound),
        ("rid", None, "md5", True, ("rid", "md5")),
        ("rid", "field", None, False, HTTPNotFound),
        ("rid", "field", None, True, ("rid", "field")),
        ("rid", "field", "md5", False, HTTPNotFound),
        ("rid", "field", "md5", True, ("rid", "field")),
    ],
)
@pytest.mark.asyncio
async def test_validate_field_upload(rid, field, md5, exists: bool, result):
    mock_uuid = Mock()
    mock_uuid4 = Mock()
    mock_uuid4.hex = "uuid4"
    mock_uuid.uuid4 = Mock(return_value=mock_uuid4)

    with (
        patch("nucliadb.writer.api.v1.upload.uuid", mock_uuid),
        patch(
            "nucliadb.writer.api.v1.upload.datamanagers.atomic.resources.resource_exists",
            AsyncMock(return_value=exists),
        ),
    ):
        if isinstance(result, tuple):
            _, result_rid, result_field = await validate_field_upload("kbid", rid, field, md5)
            assert (result_rid, result_field) == result
        else:
            with pytest.raises(result):
                _, result_rid, result_field = await validate_field_upload("kbid", rid, field, md5)
