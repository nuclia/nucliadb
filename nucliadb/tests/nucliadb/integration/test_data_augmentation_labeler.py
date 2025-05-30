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
import uuid
from typing import AsyncIterator, Iterable
from unittest.mock import patch

import pytest
from pytest_mock import MockerFixture

from nucliadb.common import datamanagers
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.processing import (
    DummyProcessingEngine,
    ProcessingEngine,
    start_processing_engine,
    stop_processing_engine,
)
from nucliadb_protos.resources_pb2 import (
    Classification,
    ExtractedTextWrapper,
    FieldComputedMetadata,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldMetadata,
    FieldMetadataDiffWrapper,
    FieldType,
)
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
    get_storage,
    get_utility,
    start_partitioning_utility,
    stop_partitioning_utility,
)


@pytest.fixture(scope="function")
def processing_utility(dummy_processing: ProcessingEngine) -> Iterable[ProcessingEngine]:
    yield dummy_processing


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


async def test_ingestion_of_field_metadata_diff(
    dummy_nidx_utility,
    knowledgebox_ingest: str,
    processor: Processor,
    partition_utility: PartitionUtility,
    processing_utility: DummyProcessingEngine,
    mocker: MockerFixture,
):
    kbid = knowledgebox_ingest
    rid = uuid.uuid4().hex

    # Resource creation (from writer)
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.slug = "my-resource"
    bm.source = BrokerMessage.MessageSource.WRITER
    bm.texts["my-text"].body = "This is my text"
    await processor.process(bm, 1)

    # Processed resource (from regular processing)
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.slug = "my-resource"
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    field = FieldID(
        field_type=FieldType.TEXT,
        field="my-text",
    )
    bm.extracted_text.append(
        ExtractedTextWrapper(
            body=ExtractedText(
                text="This is an extracted text",
            ),
            field=field,
        )
    )
    bm.field_metadata.append(
        FieldComputedMetadataWrapper(
            metadata=FieldComputedMetadata(
                metadata=FieldMetadata(
                    classifications=[Classification(labelset="ls1", label="label1")],
                )
            ),
            field=field,
        )
    )

    await processor.process(bm, 2)

    async def check_classifications(classifications: list[Classification]):
        async with datamanagers.with_ro_transaction() as txn:
            kb = KnowledgeBox(txn, storage=await get_storage(), kbid=kbid)
            resource = await kb.get(rid)
            assert resource is not None
            field = await resource.get_field(key="my-text", type=FieldType.TEXT)
            fcm = await field.get_field_metadata(force=True)
            assert fcm is not None
            assert fcm.metadata.classifications == classifications

    await check_classifications([Classification(labelset="ls1", label="label1")])

    # Data augmentation broker message, this should be like the ones generated
    # by labeller/prompt-guard/llama-guard data augmentation tasks
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    fmdw = FieldMetadataDiffWrapper()
    fmdw.field.CopyFrom(field)
    fmdw.metadata_diff.classifications.added.append(Classification(labelset="ls2", label="label2"))
    fmdw.metadata_diff.classifications.deleted_labelsets.append("ls1")
    bm.field_metadata_diff.append(fmdw)

    await processor.process(bm, 3)

    await check_classifications([Classification(labelset="ls2", label="label2")])
