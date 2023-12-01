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
from io import BytesIO
from unittest.mock import AsyncMock, Mock

import nats.errors
import pytest
from nucliadb_protos.writer_pb2 import BrokerMessage, BrokerMessageBlobReference
from starlette.requests import Request

from nucliadb.export_import.exceptions import ExportStreamExhausted
from nucliadb.export_import.importer import ExportStream
from nucliadb.export_import.models import ImportMetadata
from nucliadb.export_import.utils import (
    IteratorExportStream,
    TaskRetryHandler,
    get_cloud_files,
    import_broker_message,
    transaction_commit,
)
from nucliadb_models.export_import import Status
from nucliadb_protos import resources_pb2
from nucliadb_utils.const import Streams


@pytest.fixture(scope="function")
def transaction():
    mock = Mock()
    mock.commit = AsyncMock()
    yield mock


@pytest.fixture(scope="function")
def partitioning():
    mock = Mock()
    mock.generate_partition = Mock(return_value=1)
    yield mock


def get_cf(uri=None) -> resources_pb2.CloudFile:
    cf = resources_pb2.CloudFile()
    uri = uri or "//foo/bar"
    cf.uri = uri
    cf.source = resources_pb2.CloudFile.Source.LOCAL
    return cf


@pytest.fixture(scope="function")
def broker_message():
    bm = BrokerMessage()
    bm.kbid = "foobar"

    # Add a file field
    file = resources_pb2.FieldFile()
    file.file.CopyFrom(get_cf("file"))
    bm.files["file"].CopyFrom(file)

    # Add a conversation with an attachment
    conversation = resources_pb2.Conversation()
    message = resources_pb2.Message()
    attachment = get_cf("attachment")
    message.content.attachments.append(attachment)
    conversation.messages.append(message)
    bm.conversations["conversation"].CopyFrom(conversation)

    # Add a layout with a file
    layout = resources_pb2.FieldLayout()
    block = resources_pb2.Block()
    block.file.CopyFrom(get_cf("layout"))
    layout.body.blocks["foo"].CopyFrom(block)
    bm.layouts["layout"].CopyFrom(layout)

    # Field extracted data
    fed = resources_pb2.FileExtractedData()
    fed.file_generated["foo"].CopyFrom(get_cf("field_file_generated"))
    fed.file_preview.CopyFrom(get_cf("field_file_preview"))
    fed.file_thumbnail.CopyFrom(get_cf("field_file_thumbnail"))
    fed.file_pages_previews.pages.append(get_cf("field_file_pages_previews"))
    bm.file_extracted_data.append(fed)

    # Link extracted data
    led = resources_pb2.LinkExtractedData()
    led.link_thumbnail.CopyFrom(get_cf("link_thumbnail"))
    led.link_preview.CopyFrom(get_cf("link_preview"))
    led.link_image.CopyFrom(get_cf("link_image"))
    bm.link_extracted_data.append(led)

    # Field metadata
    fcmw = resources_pb2.FieldComputedMetadataWrapper()
    fcmw.metadata.metadata.thumbnail.CopyFrom(get_cf("metadata_thumbnail"))
    fcmw.metadata.split_metadata["foo"].thumbnail.CopyFrom(
        get_cf("metadata_split_thumbnail")
    )
    bm.field_metadata.append(fcmw)

    return bm


class ContextMock:
    def __init__(self, transaction, partitioning):
        self.transaction = transaction
        self.partitioning = partitioning


async def test_import_broker_message(broker_message, transaction, partitioning):
    context = ContextMock(transaction, partitioning)

    import_kbid = "import_kbid"
    assert broker_message.kbid != import_kbid

    await import_broker_message(context, import_kbid, broker_message)

    # Sends two messages
    assert transaction.commit.call_count == 2

    for call in transaction.commit.call_args_list:
        # Message contains import kbid
        assert call[0][0].kbid == import_kbid

        # Sends to correct topic
        assert call[1]["target_subject"] == Streams.INGEST_PROCESSED.subject


def test_get_cloud_files(broker_message):
    # All expected binaries are returned
    binaries = get_cloud_files(broker_message)
    assert len(binaries) == 12
    for cf in binaries:
        assert cf.source == resources_pb2.CloudFile.Source.LOCAL

    # Make sure that the source is set to export on the broker message cfs
    for cf in get_cloud_files(broker_message):
        assert cf.source == resources_pb2.CloudFile.Source.EXPORT


async def test_export_stream():
    export = BytesIO(b"1234567890")
    stream = ExportStream(export)
    assert stream.read_bytes == 0
    assert await stream.read(0) == b""
    assert stream.read_bytes == 0
    assert await stream.read(1) == b"1"
    assert stream.read_bytes == 1
    assert await stream.read(2) == b"23"
    assert stream.read_bytes == 3
    assert await stream.read(50) == b"4567890"
    assert stream.read_bytes == 10
    with pytest.raises(ExportStreamExhausted):
        await stream.read(1)


class DummyTestRequest(Request):
    def __init__(self, data: bytes, receive_chunk_size: int = 10):
        super().__init__(
            scope={
                "type": "http",
                "http_version": "1.1",
                "method": "GET",
                "headers": [],
            },
            receive=self.receive,
        )
        self.receive_chunk_size = receive_chunk_size
        self.bytes = BytesIO(data)

    async def receive(self):
        chunk = self.bytes.read(self.receive_chunk_size)
        more_data = True
        if chunk == b"":
            more_data = False
        return {"type": "http.request", "body": chunk, "more_body": more_data}


async def test_iterator_export_stream():
    request = DummyTestRequest(data=b"01234XYZ", receive_chunk_size=2)

    iterator = request.stream().__aiter__()
    export_stream = IteratorExportStream(iterator)
    assert await export_stream.read(0) == b""
    assert export_stream.read_bytes == 0

    for i in range(5):
        assert await export_stream.read(1) == f"{i}".encode()
    assert export_stream.read_bytes == 5

    assert await export_stream.read(3) == b"XYZ"
    assert export_stream.read_bytes == 8

    with pytest.raises(ExportStreamExhausted):
        await export_stream.read(1)

    with pytest.raises(ExportStreamExhausted):
        await export_stream.read(0)

    request = DummyTestRequest(data=b"foobar", receive_chunk_size=2)
    iterator = request.stream().__aiter__()
    export_stream = IteratorExportStream(iterator)
    assert await export_stream.read(50) == b"foobar"
    assert export_stream.read_bytes == 6

    with pytest.raises(ExportStreamExhausted):
        await export_stream.read(0)


class TestTaskRetryHandler:
    @pytest.fixture(scope="function")
    def callback(self):
        return AsyncMock()

    @pytest.fixture(scope="function")
    def dm(self):
        dm = Mock()
        dm.set_metadata = AsyncMock()
        return dm

    @pytest.fixture(scope="function")
    def metadata(self):
        return ImportMetadata(kbid="kbid", id="import_id")

    async def test_ok(self, callback, dm, metadata):
        callback.return_value = 100
        trh = TaskRetryHandler("foo", dm, metadata)
        callback_retried = trh.wrap(callback)

        result = await callback_retried("foo", bar="baz")
        assert result == 100

        callback.assert_called_once_with("foo", bar="baz")

        assert metadata.task.status == Status.FINISHED

    async def test_errors_are_retried(self, callback, dm, metadata):
        callback.side_effect = ValueError("foo")

        trh = TaskRetryHandler("foo", dm, metadata, max_tries=2)
        callback_retried = trh.wrap(callback)

        with pytest.raises(ValueError):
            await callback_retried("foo", bar="baz")

        callback.assert_called_once_with("foo", bar="baz")

        assert metadata.task.status == Status.RUNNING
        assert metadata.task.retries == 1

        with pytest.raises(ValueError):
            await callback_retried("foo", bar="baz")

        assert metadata.task.status == Status.RUNNING
        assert metadata.task.retries == 2

    async def test_ignored_statuses(self, callback, dm, metadata):
        trh = TaskRetryHandler("foo", dm, metadata)
        callback_retried = trh.wrap(callback)

        for status in (Status.ERRORED, Status.FINISHED):
            metadata.task.status = status
            await callback_retried("foo", bar="baz")
            callback.assert_not_called()


async def test_transaction_commit_sends_storage_reference_on_max_payload_error():
    context = Mock()
    context.transaction.commit = AsyncMock(
        side_effect=[nats.errors.MaxPayloadError, None]
    )
    context.blob_storage = AsyncMock()
    context.blob_storage.set_stream_message.return_value = "key"

    bm = BrokerMessage(kbid="kbid", uuid="uuid")

    await transaction_commit(context, bm, 1)

    assert context.transaction.commit.call_count == 2
    call = context.transaction.commit.call_args_list[-1]
    assert isinstance(call[1]["writer"], BrokerMessageBlobReference)
    assert call[1]["headers"] == {"X-MESSAGE-TYPE": "PROXY"}
