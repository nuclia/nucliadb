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
import hashlib
import uuid
from collections.abc import AsyncIterator, Iterable
from unittest.mock import patch

import pytest
from httpx import AsyncClient
from nidx_protos import noderesources_pb2
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
from nucliadb.models.internal.processing import Source
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    FieldID,
    FieldType,
    Paragraph,
)
from nucliadb_protos.utils_pb2 import ExtractedText
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
)
from nucliadb_protos.writer_pb2_grpc import WriterStub
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
from tests.utils import inject_message


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


async def test_send_to_process_generated_fields(
    dummy_nidx_utility,
    knowledgebox: str,
    processor: Processor,
    partition_utility: PartitionUtility,
    processing_utility: DummyProcessingEngine,
    mocker: MockerFixture,
):
    kbid = knowledgebox
    rid = uuid.uuid4().hex

    # Resource creation (from writer)
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.slug = "my-resource"
    bm.source = BrokerMessage.MessageSource.WRITER
    bm.texts["my-text"].body = "This is my text"
    await processor.process(bm, 1)

    # Processed resource (from processing)
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

    # Data augmentation broker message, this should be like the ones generated
    # by ask data augmentation task
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    da_field = "da-author-f-0-0"
    bm.texts[da_field].body = "Text author"
    bm.texts[da_field].md5 = hashlib.md5(b"Text author").hexdigest()
    bm.texts[da_field].generated_by.data_augmentation.SetInParent()

    await processor.process(bm, 3)

    assert len(processing_utility.calls) == 1
    send_to_process_call = processing_utility.calls[0]
    payload, partition = send_to_process_call
    assert payload.uuid == rid
    assert payload.source == Source.INGEST
    assert payload.textfield[da_field].body == bm.texts[da_field].body
    assert partition == 1

    async with datamanagers.with_ro_transaction() as txn:
        kb = KnowledgeBox(txn, storage=await get_storage(), kbid=kbid)
        resource = await kb.get(rid)
        assert resource is not None
        field = await resource.get_field(da_field, FieldType.TEXT)
        generated_by = await field.generated_by()
        assert generated_by.WhichOneof("author") == "data_augmentation"

    # Processed DA resource (from processing)
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    bm.extracted_text.append(
        ExtractedTextWrapper(
            body=ExtractedText(
                text="Extracted text for author",
            ),
            field=FieldID(
                field_type=FieldType.TEXT,
                field=da_field,
            ),
        )
    )

    index_resource_spy = mocker.spy(processor.index_node_shard_manager, "add_resource")
    await processor.process(bm, 4)

    index_message: noderesources_pb2.Resource = index_resource_spy.call_args.args[1]
    assert index_message.resource.uuid == rid
    # label for generated fields from data augmentation is present
    assert "/g/da/author" in index_message.texts[f"t/{da_field}"].labels


@pytest.mark.deploy_modes("standalone")
async def test_data_augmentation_field_generation_and_search(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    kbid = standalone_knowledgebox
    slug = "my-resource"
    field_id = "my-text"

    # Resource creation (from writer)
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My resources",
            "slug": slug,
            "texts": {
                field_id: {
                    "body": "This is my text",
                    "format": "PLAIN",
                }
            },
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Processed resource (from processing)
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.slug = slug
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    field_id_pb = FieldID(
        field_type=FieldType.TEXT,
        field=field_id,
    )
    bm.extracted_text.append(
        ExtractedTextWrapper(
            body=ExtractedText(
                text="This is an extracted text",
            ),
            field=field_id_pb,
        )
    )
    field_metadata = resources_pb2.FieldComputedMetadataWrapper()
    field_metadata.field.CopyFrom(field_id_pb)
    field_metadata.metadata.metadata.paragraphs.append(Paragraph(start=0, end=25))
    bm.field_metadata.append(field_metadata)
    await inject_message(nucliadb_ingest_grpc, bm)

    # Data augmentation broker message
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    da_field_id = "da-author-f-0-0"
    bm.texts[da_field_id].body = "Text author"
    bm.texts[da_field_id].md5 = hashlib.md5(b"Text author").hexdigest()
    bm.texts[da_field_id].generated_by.data_augmentation.SetInParent()
    await inject_message(nucliadb_ingest_grpc, bm)

    # Processed DA resource (from processing)
    bm = BrokerMessage()
    bm.kbid = kbid
    bm.uuid = rid
    bm.slug = slug
    bm.source = BrokerMessage.MessageSource.PROCESSOR
    da_field_id_pb = FieldID(
        field_type=FieldType.TEXT,
        field=da_field_id,
    )
    bm.extracted_text.append(
        ExtractedTextWrapper(
            body=ExtractedText(
                text="Extracted text for da author",
            ),
            field=da_field_id_pb,
        )
    )
    field_metadata = resources_pb2.FieldComputedMetadataWrapper()
    field_metadata.field.CopyFrom(da_field_id_pb)
    field_metadata.metadata.metadata.paragraphs.append(Paragraph(start=0, end=28))
    bm.field_metadata.append(field_metadata)
    await inject_message(nucliadb_ingest_grpc, bm)

    # Now validate we can search and filter out data augmentation fields
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "text",
            "min_score": {"bm25": 0.0},
        },
    )
    assert resp.status_code == 200
    unfiltered = resp.json()
    assert unfiltered["total"] == 2
    assert unfiltered["resources"][rid]["fields"].keys() == {f"/t/{field_id}", f"/t/{da_field_id}"}

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "text",
            "filters": [{"none": ["/g/da"]}],
            "min_score": {"bm25": 0.0},
        },
    )
    assert resp.status_code == 200
    filtered_out = resp.json()
    assert filtered_out["total"] == 1
    assert filtered_out["resources"][rid]["fields"].keys() == {f"/t/{field_id}"}

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "text",
            "filters": [{"none": ["/generated.data-augmentation"]}],
            "min_score": {"bm25": 0.0},
        },
    )
    assert resp.status_code == 200
    filtered_out = resp.json()
    assert filtered_out["total"] == 1
    assert filtered_out["resources"][rid]["fields"].keys() == {f"/t/{field_id}"}

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "text",
            "filters": [{"none": ["/generated.data-augmentation/author"]}],
            "min_score": {"bm25": 0.0},
        },
    )
    assert resp.status_code == 200
    filtered_out = resp.json()
    assert filtered_out["total"] == 1
    assert filtered_out["resources"][rid]["fields"].keys() == {f"/t/{field_id}"}

    # Test for filter expression filters
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "text",
            "filter_expression": {"field": {"not": {"prop": "generated", "by": "data-augmentation"}}},
            "min_score": {"bm25": 0.0},
        },
    )
    assert resp.status_code == 200
    filtered_out = resp.json()
    assert filtered_out["total"] == 1
    assert filtered_out["resources"][rid]["fields"].keys() == {f"/t/{field_id}"}

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={
            "query": "text",
            "filter_expression": {
                "field": {"not": {"prop": "generated", "by": "data-augmentation", "da_task": "author"}}
            },
            "min_score": {"bm25": 0.0},
        },
    )
    assert resp.status_code == 200
    filtered_out = resp.json()
    assert filtered_out["total"] == 1
    assert filtered_out["resources"][rid]["fields"].keys() == {f"/t/{field_id}"}
