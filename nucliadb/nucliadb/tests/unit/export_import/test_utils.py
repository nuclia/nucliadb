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
from unittest.mock import AsyncMock, Mock

import pytest
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.export_import.utils import get_cloud_files, import_broker_message
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
