from unittest.mock import AsyncMock, MagicMock

import pytest
from nidx_protos import noderesources_pb2

from nucliadb.ingest.orm.metrics import processor_observer
from nucliadb.ingest.orm.processor import processor as processor_helpers
from nucliadb.ingest.orm.processor import processor_marklogic
from nucliadb_protos import writer_pb2


def sample_value(metric, name, stage):
    return sum(
        sample.value
        for family in metric.collect()
        for sample in family.samples
        if sample.name == name and sample.labels.get("type") == stage
    )


@pytest.mark.parametrize(
    "method,stage",
    [
        ("delete", "delete_resource"),
        ("txn", "txn"),
        ("create_resource", "apply_resource"),
        ("create_resource", "commit_slug"),
        ("update_resource", "apply_resource"),
        ("create_fields", "apply_fields"),
        ("create_fields", "apply_field_values"),
        ("update_fields", "apply_fields"),
        ("handle_data_augmentation_fields", "handle_data_augmentation_fields"),
        ("generate_index_message", "generate_index_message"),
        ("index_resource", "index_resource"),
        ("persist_fields_processed_data", "apply_field_extracted_data"),
        ("persist_fields_processed_data", "apply_fields"),
    ],
)
async def test_marklogic_processor_emits_legacy_metrics(method, stage, mocker):
    processor = processor_marklogic.ProcessorMarkLogic(MagicMock(), MagicMock())
    resource = MagicMock()
    resource.apply_field_extracted_data = AsyncMock()
    mocker.patch.object(processor, "orm_resource", return_value=resource)
    mocker.patch.object(processor, "apply_extracted_basic_updates", new=AsyncMock())
    mocker.patch.object(
        processor,
        "ensure_kb_and_resource",
        new=AsyncMock(side_effect=processor_marklogic.ResourceDoesNotExist()),
    )
    mocker.patch.object(processor_marklogic.datamanagers.resources, "create_resource", new=AsyncMock())
    mocker.patch.object(processor_marklogic.datamanagers.resources, "update_resource", new=AsyncMock())
    mocker.patch.object(
        processor_marklogic, "get_field_processed_contents", new=AsyncMock(return_value=[])
    )
    builder = MagicMock()
    builder.for_writer_bm = AsyncMock(return_value=noderesources_pb2.Resource())
    mocker.patch.object(processor_marklogic, "IndexMessageBuilderV2", return_value=builder)
    message = writer_pb2.BrokerMessage(kbid="kb", uuid="rid")
    message.basic.SetInParent()
    arguments = {
        "delete": (message, 1, "1", False),
        "txn": (message, 1, "1", False),
        "create_resource": (MagicMock(), message, "kb", "rid"),
        "update_resource": (MagicMock(), writer_pb2.BrokerMessage(), "kb", "rid"),
        "create_fields": (MagicMock(), message, "kb", "rid"),
        "update_fields": (MagicMock(), message, "kb", "rid"),
        "handle_data_augmentation_fields": (message, "kb", "rid"),
        "generate_index_message": (MagicMock(), "rid", message, True),
        "index_resource": (MagicMock(), "kb", "rid", noderesources_pb2.Resource(), True),
        "persist_fields_processed_data": (MagicMock(), "kb", "rid", message, True),
    }
    before_count = sample_value(
        processor_observer.counter, "nucliadb_ingest_processor_count_total", stage
    )
    before_duration = sample_value(
        processor_observer.histogram, "nucliadb_ingest_processor_duration_seconds_count", stage
    )

    await getattr(processor, method)(*arguments[method])

    assert (
        sample_value(processor_observer.counter, "nucliadb_ingest_processor_count_total", stage)
        == before_count + 1
    )
    assert (
        sample_value(
            processor_observer.histogram, "nucliadb_ingest_processor_duration_seconds_count", stage
        )
        == before_duration + 1
    )


def test_legacy_processor_class_is_removed():
    import nucliadb.ingest.orm.processor as package

    assert not hasattr(package, "Processor")
    assert not hasattr(processor_helpers, "Processor")


async def test_failed_delete_uses_legacy_metric_labels(mocker):
    processor = processor_marklogic.ProcessorMarkLogic(MagicMock(), MagicMock())
    mocker.patch.object(
        processor, "ensure_kb_and_resource", new=AsyncMock(side_effect=ValueError("invalid"))
    )
    labels = {**processor_observer.labels, "type": "delete_resource", "status": "error"}
    counter = processor_observer.counter.labels(**labels)
    before = next(
        sample.value
        for family in counter.collect()
        for sample in family.samples
        if sample.name.endswith("_total")
    )

    with pytest.raises(ValueError, match="invalid"):
        await processor.delete(writer_pb2.BrokerMessage(), 1, "1", False)

    after = next(
        sample.value
        for family in counter.collect()
        for sample in family.samples
        if sample.name.endswith("_total")
    )
    assert after == before + 1
