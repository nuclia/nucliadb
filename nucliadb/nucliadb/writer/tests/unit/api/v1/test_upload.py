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
from fastapi.requests import Request

from nucliadb.ingest.processing import ProcessingInfo, Source
from nucliadb.writer.api.v1.upload import store_file_on_nuclia_db
from nucliadb_models.resource import QueueType

UPLOAD_PACKAGE = "nucliadb.writer.api.v1.upload"


@pytest.fixture(scope="function")
def partitioning_mock():
    with mock.patch(f"{UPLOAD_PACKAGE}.get_partitioning"):
        yield


@pytest.fixture(scope="function")
def processing_mock():
    with mock.patch(f"{UPLOAD_PACKAGE}.get_processing") as get_processing_mock:
        processing = mock.AsyncMock()
        processing.convert_internal_filefield_to_str.return_value = "foo"
        processing_info = ProcessingInfo(seqid=1, account_seq=1, queue=QueueType.SHARED)
        processing.send_to_process.return_value = processing_info
        get_processing_mock.return_value = processing
        yield processing


@pytest.fixture(scope="function")
def transaction_mock():
    with mock.patch(f"{UPLOAD_PACKAGE}.get_transaction") as transaction_mock:
        transaction = mock.AsyncMock()
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
