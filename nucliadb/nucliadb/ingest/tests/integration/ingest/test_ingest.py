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
from datetime import datetime
from os.path import dirname, getsize
from unittest.mock import patch
from uuid import uuid4

import nats
import pytest
from nats.aio.client import Client
from nats.js import JetStreamContext
from nucliadb_protos.audit_pb2 import AuditField, AuditRequest
from nucliadb_protos.resources_pb2 import (
    TEXT,
    Answers,
    Classification,
    CloudFile,
    Entity,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldQuestionAnswerWrapper,
    FieldType,
    FileExtractedData,
    LargeComputedMetadataWrapper,
)
from nucliadb_protos.resources_pb2 import Metadata as PBMetadata
from nucliadb_protos.resources_pb2 import Origin, Paragraph, QuestionAnswer
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.ingest import SERVICE_NAME
from nucliadb.ingest.consumer.auditing import (
    IndexAuditHandler,
    ResourceWritesAuditHandler,
)
from nucliadb.ingest.orm.exceptions import DeadletteredError
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.orm.resource import Resource
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import Utility, get_indexing, get_storage, set_utility

EXAMPLE_VECTOR = base64.b64decode(
    "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"  # noqa
)


@pytest.fixture(autouse=True)
async def audit_consumers(
    maindb_driver, gcs_storage, pubsub, stream_audit: StreamAuditStorage
):
    index_auditor = IndexAuditHandler(
        driver=maindb_driver,
        audit=stream_audit,
        pubsub=pubsub,
    )
    resource_writes_auditor = ResourceWritesAuditHandler(
        driver=maindb_driver,
        storage=gcs_storage,
        audit=stream_audit,
        pubsub=pubsub,
    )

    await index_auditor.initialize()
    await resource_writes_auditor.initialize()
    yield
    await index_auditor.finalize()
    await resource_writes_auditor.finalize()


@pytest.fixture()
def kbid(
    local_files,
    gcs_storage: Storage,
    txn,
    cache,
    fake_node,
    processor,
    knowledgebox_ingest,
):
    yield knowledgebox_ingest


@pytest.mark.asyncio
async def test_ingest_messages_autocommit(kbid: str, processor):
    rid = str(uuid.uuid4())
    message1: BrokerMessage = BrokerMessage(
        kbid=kbid,
        uuid=rid,
        slug="slug1",
        type=BrokerMessage.AUTOCOMMIT,
    )
    filename = f"{dirname(__file__)}/assets/file.png"
    cf1 = CloudFile(
        uri="file.png",
        source=CloudFile.Source.LOCAL,
        bucket_name="/integration/ingest/assets",
        size=getsize(filename),
        content_type="image/png",
        filename="file.png",
    )
    message1.basic.icon = "text/plain"
    message1.basic.title = "Title Resource"
    message1.basic.summary = "Summary of Document"
    message1.basic.thumbnail = "doc"
    message1.basic.layout = "default"
    message1.basic.metadata.language = "es"
    message1.basic.created.FromDatetime(datetime.now())
    message1.basic.modified.FromDatetime(datetime.now())
    message1.origin.source = Origin.Source.WEB
    message1.files["file"].file.CopyFrom(cf1)

    fed = FileExtractedData()
    fed.file_pages_previews.pages.append(cf1)
    fed.language = "ca"
    fed.md5 = "asdsadsad"
    fed.metadata["key1"] = "ca"
    fed.nested["key2"] = "ca"
    fed.file_generated["subfile1"].CopyFrom(cf1)
    fed.file_preview.CopyFrom(cf1)
    fed.file_thumbnail.CopyFrom(cf1)
    message1.file_extracted_data.append(fed)

    etw = ExtractedTextWrapper()
    etw.body.text = "My own text"
    etw.field.field = "file"
    etw.field.field_type = FieldType.FILE
    message1.extracted_text.append(etw)
    etw = ExtractedTextWrapper()
    etw.body.text = "My summary"
    etw.field.field = "summary"
    etw.field.field_type = FieldType.GENERIC
    message1.extracted_text.append(etw)

    fcm = FieldComputedMetadataWrapper()
    fcm.field.field = "file"
    fcm.field.field_type = FieldType.FILE
    p1 = Paragraph(
        start=1,
        end=20,
    )
    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.ner["Ramon"] = "PERSON"

    c1 = Classification()
    c1.label = "label1"
    c1.labelset = "labelset1"
    fcm.metadata.metadata.classifications.append(c1)
    message1.field_metadata.append(fcm)

    lcmw = LargeComputedMetadataWrapper()
    lcmw.field.field = "file"
    lcmw.field.field_type = FieldType.FILE
    lcmw.real.metadata.tokens["asd"] = 4
    lcmw.real.metadata.entities.append(Entity(token="token", root="tok", type="PERSON"))
    message1.field_large_metadata.append(lcmw)

    ev = ExtractedVectorsWrapper()
    ev.field.field = "file"
    ev.field.field_type = FieldType.FILE
    v1 = Vector(
        start=1, end=10, start_paragraph=1, end_paragraph=20, vector=EXAMPLE_VECTOR
    )
    ev.vectors.vectors.vectors.append(v1)
    message1.field_vectors.append(ev)

    message1.source = BrokerMessage.MessageSource.WRITER
    await processor.process(message=message1, seqid=1)

    index = get_indexing()
    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(index._calls[0][1])
    assert pb.texts["a/summary"].text == "My summary"  # type: ignore

    pb = await storage.get_indexing(index._calls[1][1])
    assert pb.texts["a/summary"].text == "My summary"  # type: ignore


@pytest.mark.asyncio
async def test_ingest_error_message(
    kbid: str, gcs_storage: Storage, processor, maindb_driver
):
    filename = f"{dirname(__file__)}/assets/resource.pb"
    with open(filename, "r") as f:
        data = base64.b64decode(f.read())
    message0: BrokerMessage = BrokerMessage()
    message0.ParseFromString(data)
    message0.kbid = kbid
    message0.source = BrokerMessage.MessageSource.WRITER

    await processor.process(message=message0, seqid=1)

    filename = f"{dirname(__file__)}/assets/error.pb"
    with open(filename, "r") as f:
        data = base64.b64decode(f.read())
    message1: BrokerMessage = BrokerMessage()
    message1.ParseFromString(data)
    message1.kbid = kbid
    message1.ClearField("field_vectors")
    message1.source = BrokerMessage.MessageSource.WRITER

    await processor.process(message=message1, seqid=2)

    async with maindb_driver.transaction() as txn:
        kb_obj = KnowledgeBox(txn, gcs_storage, kbid=kbid)
        r = await kb_obj.get(message1.uuid)
        assert r is not None
        field_obj = await r.get_field("wikipedia_ml", TEXT)
        ext1 = await field_obj.get_extracted_text()
        lfm1 = await field_obj.get_large_field_metadata()
        fm1 = await field_obj.get_field_metadata()
        basic = await r.get_basic()
        assert basic is not None
        assert basic.slug == message1.slug
        assert basic.summary == message0.basic.summary

        assert ext1.text == message1.extracted_text[0].body.text

        assert lfm1 is not None
        assert fm1 is not None
        assert field_obj.value.body == message0.texts["wikipedia_ml"].body


@pytest.mark.asyncio
async def test_ingest_messages_origin(
    local_files,
    gcs_storage: Storage,
    fake_node,
    processor,
    knowledgebox_ingest,
):
    rid = "43ece3e4-b706-4c74-b41b-3637f6d28197"
    message1: BrokerMessage = BrokerMessage(
        kbid=knowledgebox_ingest,
        uuid=rid,
        slug="slug1",
        type=BrokerMessage.AUTOCOMMIT,
    )
    message1.source = BrokerMessage.MessageSource.WRITER
    await processor.process(message=message1, seqid=1)

    async with processor.driver.transaction() as txn:
        storage = await get_storage(service_name=SERVICE_NAME)
        kb = KnowledgeBox(txn, storage, knowledgebox_ingest)
        res = Resource(txn, storage, kb, rid)
        origin = await res.get_origin()

    # should not be set
    assert origin is None

    # now set the origin
    message1.origin.CopyFrom(
        Origin(
            source=Origin.Source.API,
            filename="file.png",
            url="http://www.google.com",
        )
    )
    await processor.process(message=message1, seqid=2)

    async with processor.driver.transaction() as txn:
        kb = KnowledgeBox(txn, storage, knowledgebox_ingest)
        res = Resource(txn, storage, kb, rid)
        origin = await res.get_origin()

    assert origin is not None
    assert origin.url == "http://www.google.com"
    assert origin.source == Origin.Source.API
    assert origin.filename == "file.png"


def add_filefields(message, items=None):
    items = items or []
    for fieldid, filename in items:
        file_path = f"{dirname(__file__)}/assets/{filename}"
        cf1 = CloudFile(
            uri=filename,
            source=CloudFile.Source.LOCAL,
            bucket_name="/integration/ingest/assets",
            size=getsize(file_path),
            content_type="application/octet-stream",
            filename=filename,
        )
        message.files[fieldid].file.CopyFrom(cf1)


def add_textfields(message, items=None):
    items = items or []
    for fieldid in items:
        message.texts[fieldid].body = "some random text"


def make_message(
    kbid: str, rid: str, slug: str = "resource", message_type=BrokerMessage.AUTOCOMMIT
):
    message: BrokerMessage = BrokerMessage(
        kbid=kbid,
        uuid=rid,
        slug=slug,
        type=message_type,
    )
    message.basic.icon = "text/plain"
    message.basic.title = "Title Resource"
    message.basic.summary = "Summary of document"
    message.basic.thumbnail = "doc"
    message.basic.layout = "default"
    message.basic.metadata.language = "es"
    message.basic.created.FromDatetime(datetime.now())
    message.basic.modified.FromDatetime(datetime.now())
    message.origin.source = Origin.Source.WEB

    return message


async def get_audit_messages(sub):
    msg = await sub.fetch(1)
    auditreq = AuditRequest()
    auditreq.ParseFromString(msg[0].data)
    return auditreq


@pytest.mark.asyncio
async def test_ingest_audit_stream_files_only(
    local_files,
    gcs_storage: Storage,
    txn,
    cache,
    fake_node,
    knowledgebox_ingest,
    stream_processor,
    stream_audit: StreamAuditStorage,
    maindb_driver,
):
    from nucliadb_utils.settings import audit_settings

    # Prepare a test audit stream to receive our messages
    partition = stream_audit.get_partition(knowledgebox_ingest)
    client: Client = await nats.connect(stream_audit.nats_servers)
    jetstream: JetStreamContext = client.jetstream()
    if audit_settings.audit_jetstream_target is None:
        assert False, "Missing jetstream target in audit settings"
    subject = audit_settings.audit_jetstream_target.format(
        partition=partition, type="*"
    )
    try:
        await jetstream.delete_stream(name=audit_settings.audit_stream)
    except nats.js.errors.NotFoundError:
        pass
    await jetstream.add_stream(name=audit_settings.audit_stream, subjects=[subject])
    psub = await jetstream.pull_subscribe(subject, "psub")

    rid = str(uuid.uuid4())

    # We use the same file multiple times, so the size will be the same
    test_png_size = getsize(f"{dirname(__file__)}/assets/file.png")
    test_text_size = getsize(f"{dirname(__file__)}/assets/text.pb")
    test_vectors_size = getsize(f"{dirname(__file__)}/assets/vectors.pb")

    #
    # Test 1: add a resource with some files
    #
    message = make_message(knowledgebox_ingest, rid)
    add_filefields(
        message,
        [("file_1", "file.png"), ("file_2", "text.pb"), ("file_3", "vectors.pb")],
    )
    await stream_processor.process(message=message, seqid=1)

    auditreq = await get_audit_messages(psub)

    # Minimal assert to make sure we get the information from the node on the audit
    # gets from the sidecar to the audit report when adding or modifying a resource
    # The values are hardcoded on nucliadb/nucliadb/ingest/orm/grpc_node_dummy.py

    assert auditreq.kbid == knowledgebox_ingest
    assert auditreq.rid == rid
    assert auditreq.type == AuditRequest.AuditType.NEW

    try:
        int(auditreq.trace_id)
    except ValueError:
        assert False, "Invalid trace ID"

    audit_by_fieldid = {audit.field_id: audit for audit in auditreq.fields_audit}
    assert audit_by_fieldid["file_1"].action == AuditField.FieldAction.MODIFIED
    assert audit_by_fieldid["file_1"].size == test_png_size
    assert audit_by_fieldid["file_2"].action == AuditField.FieldAction.MODIFIED
    assert audit_by_fieldid["file_2"].size == test_text_size
    assert audit_by_fieldid["file_3"].action == AuditField.FieldAction.MODIFIED
    assert audit_by_fieldid["file_3"].size == test_vectors_size

    #
    # Test 2: delete one of the previous field on the same resource
    #

    message.files.clear()
    fieldid = FieldID(field="file_1", field_type=FieldType.FILE)
    message.delete_fields.append(fieldid)

    await stream_processor.process(message=message, seqid=2)
    auditreq = await get_audit_messages(psub)

    # Minimal assert to make sure we get the information from the node on the audit
    # gets from the sidecar to the audit report when adding or modifying a resource
    # The values are hardcoded on nucliadb/nucliadb/ingest/orm/grpc_node_dummy.py

    assert auditreq.kbid == knowledgebox_ingest
    assert auditreq.rid == rid
    assert auditreq.type == AuditRequest.AuditType.MODIFIED

    #
    # Test 3: modify a file while adding and deleting other files
    #

    message = make_message(knowledgebox_ingest, rid)
    add_filefields(message, [("file_2", "file.png"), ("file_4", "text.pb")])
    fieldid = FieldID(field="file_3", field_type=FieldType.FILE)
    message.delete_fields.append(fieldid)

    await stream_processor.process(message=message, seqid=3)
    auditreq = await get_audit_messages(psub)

    # Minimal assert to make sure we get the information from the node on the audit
    # gets from the sidecar to the audit report when adding or modifying a resource
    # The values are hardcoded on nucliadb/nucliadb/ingest/orm/grpc_node_dummy.py

    assert auditreq.kbid == knowledgebox_ingest
    assert auditreq.rid == rid
    assert auditreq.type == AuditRequest.AuditType.MODIFIED

    audit_by_fieldid = {audit.field_id: audit for audit in auditreq.fields_audit}
    assert audit_by_fieldid["file_2"].action == AuditField.FieldAction.MODIFIED
    assert audit_by_fieldid["file_2"].size == test_png_size
    assert audit_by_fieldid["file_4"].action == AuditField.FieldAction.MODIFIED
    assert audit_by_fieldid["file_4"].size == test_text_size
    assert audit_by_fieldid["file_3"].action == AuditField.FieldAction.DELETED
    assert audit_by_fieldid["file_3"].size == 0

    #
    # Test 4: delete resource
    #

    message = make_message(
        knowledgebox_ingest, rid, message_type=BrokerMessage.MessageType.DELETE
    )
    await stream_processor.process(message=message, seqid=4)
    auditreq = await get_audit_messages(psub)

    assert auditreq.type == AuditRequest.AuditType.DELETED

    # Test 5: Delete knowledgebox

    txn = await maindb_driver.begin()
    kb = await KnowledgeBox.get_kb(txn, knowledgebox_ingest)

    set_utility(Utility.AUDIT, stream_audit)
    await KnowledgeBox.delete_kb(txn, kb.slug, knowledgebox_ingest)  # type: ignore

    auditreq = await get_audit_messages(psub)
    assert auditreq.kbid == knowledgebox_ingest
    assert auditreq.type == AuditRequest.AuditType.KB_DELETED

    try:
        int(auditreq.trace_id)
    except ValueError:
        assert False, "Invalid trace ID"

    # Currently where not updating audit counters on delete operations
    assert not auditreq.HasField("kb_counter")

    await txn.abort()

    await client.drain()
    await client.close()


@pytest.mark.asyncio
async def test_qa(
    local_files,
    gcs_storage: Storage,
    cache,
    fake_node,
    stream_processor,
    stream_audit: StreamAuditStorage,
    test_resource: Resource,
):
    kbid = test_resource.kb.kbid
    rid = test_resource.uuid
    driver = stream_processor.driver
    message = make_message(kbid, rid)
    message.account_seq = 2
    message.files["qa"].file.uri = "http://something"
    message.files["qa"].file.size = 123
    message.files["qa"].file.source = CloudFile.Source.LOCAL

    qaw = FieldQuestionAnswerWrapper()
    qaw.field.field_type = FieldType.FILE
    qaw.field.field = "qa"

    for i in range(10):
        qa = QuestionAnswer()

        qa.question.text = f"My question {i}"
        qa.question.language = "catalan"
        qa.question.ids_paragraphs.extend([f"id1/{i}", f"id2/{i}"])

        answer = Answers()
        answer.text = f"My answer {i}"
        answer.language = "catalan"
        answer.ids_paragraphs.extend([f"id1/{i}", f"id2/{i}"])
        qa.answers.append(answer)
        qaw.question_answers.question_answer.append(qa)

    message.question_answers.append(qaw)

    await stream_processor.process(message=message, seqid=1)

    async with driver.transaction() as txn:
        kb_obj = KnowledgeBox(txn, gcs_storage, kbid=kbid)
        r = await kb_obj.get(message.uuid)
        assert r is not None
        res = await r.get_field(key="qa", type=FieldType.FILE)
        res_qa = await res.get_question_answers()

    assert qaw.question_answers == res_qa

    # delete op
    message = make_message(kbid, rid, message_type=BrokerMessage.MessageType.DELETE)
    await stream_processor.process(message=message, seqid=2)


@pytest.mark.asyncio
async def test_ingest_audit_stream_mixed(
    local_files,
    gcs_storage: Storage,
    cache,
    fake_node,
    stream_processor,
    stream_audit: StreamAuditStorage,
    test_resource: Resource,
):
    from nucliadb_utils.settings import audit_settings

    kbid = test_resource.kb.kbid
    rid = test_resource.uuid
    # Prepare a test audit stream to receive our messages
    partition = stream_audit.get_partition(kbid)
    client: Client = await nats.connect(stream_audit.nats_servers)
    jetstream: JetStreamContext = client.jetstream()
    if audit_settings.audit_jetstream_target is None:
        assert False, "Missing jetstream target in audit settings"
    subject = audit_settings.audit_jetstream_target.format(
        partition=partition, type="*"
    )
    try:
        await jetstream.delete_stream(name=audit_settings.audit_stream)
    except nats.js.errors.NotFoundError:
        pass
    await jetstream.add_stream(name=audit_settings.audit_stream, subjects=[subject])
    psub = await jetstream.pull_subscribe(subject, "psub")

    #
    # Test 1: starting with a complete resource, do one of heac add, mod, del field
    #
    message = make_message(kbid, rid)
    add_filefields(message, [("file_1", "file.png")])
    add_textfields(message, ["text1"])
    fieldid = FieldID(field="conv1", field_type=FieldType.CONVERSATION)
    message.delete_fields.append(fieldid)
    await stream_processor.process(message=message, seqid=1)

    auditreq = await get_audit_messages(psub)

    # Minimal assert to make sure we get the information from the node on the audit
    # gets from the sidecar to the audit report when adding or modifying a resource
    # The values are hardcoded on nucliadb/nucliadb/ingest/orm/grpc_node_dummy.py

    assert auditreq.kbid == kbid
    assert auditreq.rid == rid
    assert auditreq.type == AuditRequest.AuditType.MODIFIED

    assert len(auditreq.fields_audit) == 4
    audit_by_fieldid = {audit.field_id: audit for audit in auditreq.fields_audit}
    assert audit_by_fieldid["file_1"].action == AuditField.FieldAction.MODIFIED
    assert audit_by_fieldid["text1"].action == AuditField.FieldAction.MODIFIED
    assert audit_by_fieldid["conv1"].action == AuditField.FieldAction.DELETED

    #
    # Test 2: delete resource
    #

    message = make_message(kbid, rid, message_type=BrokerMessage.MessageType.DELETE)
    await stream_processor.process(message=message, seqid=2)
    auditreq = await get_audit_messages(psub)

    assert auditreq.type == AuditRequest.AuditType.DELETED

    await client.drain()
    await client.close()


@pytest.mark.asyncio
async def test_ingest_account_seq_stored(
    local_files,
    gcs_storage: Storage,
    fake_node,
    stream_processor,
    test_resource: Resource,
):
    driver = stream_processor.driver
    kbid = test_resource.kb.kbid
    rid = test_resource.uuid

    message = make_message(kbid, rid)
    message.account_seq = 2
    add_filefields(message, [("file_1", "file.png")])
    await stream_processor.process(message=message, seqid=1)

    async with driver.transaction() as txn:
        kb_obj = KnowledgeBox(txn, gcs_storage, kbid=kbid)
        r = await kb_obj.get(message.uuid)
        assert r is not None
        basic = await r.get_basic()

    assert basic is not None
    assert basic.last_account_seq == 2
    assert basic.queue == 0


@pytest.mark.asyncio
async def test_ingest_processor_handles_missing_kb(
    local_files,
    gcs_storage: Storage,
    fake_node,
    stream_processor,
    test_resource: Resource,
):
    kbid = str(uuid4())
    rid = str(uuid4())
    message = make_message(kbid, rid)
    message.account_seq = 1
    await stream_processor.process(message=message, seqid=1)


@pytest.mark.asyncio
async def test_ingest_autocommit_deadletter_marks_resource(
    kbid: str, processor: Processor, gcs_storage, maindb_driver
):
    rid = str(uuid.uuid4())
    message = make_message(kbid, rid)

    with patch.object(processor, "notify_commit") as mock_notify, pytest.raises(
        DeadletteredError
    ):
        # cause an error to force deadletter handling
        mock_notify.side_effect = Exception("test")
        await processor.process(message=message, seqid=1)

    async with maindb_driver.transaction() as txn:
        kb_obj = KnowledgeBox(txn, gcs_storage, kbid=kbid)
        resource = await kb_obj.get(message.uuid)

    mock_notify.assert_called_once()
    assert resource.basic.metadata.status == PBMetadata.Status.ERROR  # type: ignore
