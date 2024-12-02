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
import base64
import uuid
from typing import AsyncIterator, Iterable
from unittest.mock import patch

import pytest

from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.processing import (
    DummyProcessingEngine,
    ProcessingEngine,
    Source,
    start_processing_engine,
    stop_processing_engine,
)
from nucliadb_protos.resources_pb2 import ExtractedTextWrapper, FieldID, FieldType
from nucliadb_protos.utils_pb2 import ExtractedText
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
)
from nucliadb_utils.partition import PartitionUtility
from nucliadb_utils.settings import (
    nuclia_settings,
)
from nucliadb_utils.utilities import (
    Utility,
    get_utility,
    start_partitioning_utility,
    stop_partitioning_utility,
)


@pytest.fixture(scope="function")
def processing_utility(dummy_processing: ProcessingEngine) -> Iterable[ProcessingEngine]:
    yield dummy_processing


@pytest.mark.asyncio
@pytest.fixture(scope="function")
async def dummy_processing() -> AsyncIterator[ProcessingEngine]:
    with patch.object(nuclia_settings, "dummy_processing", True):
        await start_processing_engine()
        processing = get_utility(Utility.PROCESSING)
        assert processing is not None, "we expect start_processing_engine to set the utility"

        yield processing

        await stop_processing_engine()


@pytest.fixture(scope="function")
def partition_utility() -> Iterable[PartitionUtility]:
    util = start_partitioning_utility()
    yield util
    stop_partitioning_utility()


async def test_send_to_process_generated_fields(
    fake_node,
    knowledgebox_ingest: str,
    processor: Processor,
    partition_utility: PartitionUtility,
    processing_utility: DummyProcessingEngine,
):
    kbid = knowledgebox_ingest
    rid = uuid.uuid4().hex

    # Resource creation
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.slug = "my-resource"
    bm.source = BrokerMessage.MessageSource.WRITER
    bm.texts["my-text"].body = "This is my text"
    await processor.process(bm, 1)

    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.slug = "my-resource"
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.extracted_text.append(
        ExtractedTextWrapper(
            body=ExtractedText(
                text="This is an extracted text",
            ),
            field=FieldID(
                field_type=FieldType.TEXT,
                field="my-text",
            ),
        )
    )
    await processor.process(bm, 2)

    # Data augmentation broker message
    bm = BrokerMessage()
    # content from learning test for ask data augmentation task
    bm.ParseFromString(
        base64.b64decode(
            "CiRlODJjZTM5YS0wOTNjLTQzNzgtOWQ5OS1lZDc1MTk0NjVkZGQaIGUwMWMxYmYzNWMwNTY1OTFkYzMxYTA3ZjFhNThmMDRhalQKD2RhLWF1dGhvci1mLTAtMBJBCh0iIEZlbGl4IE9icmFkXHUwMGYzIENhcnJpZWRvIhogYWE1YTRkN2EwYjFlNjE3OGY0YzgzMDkxOTRkNTliZTg="
        )
    )  # noqa: E501
    bm.kbid = kbid
    bm.uuid = rid
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    da_field = "da-author-f-0-0"
    assert da_field in bm.texts
    await processor.process(bm, 3)

    assert len(processing_utility.calls) == 1
    send_to_process_call = processing_utility.calls[0]
    payload, partition = send_to_process_call
    assert payload.uuid == rid
    assert payload.source == Source.INGEST
    assert payload.textfield[da_field].body == bm.texts[da_field].body
    assert partition == 1
