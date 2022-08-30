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

import nats
import pytest
from nats.aio.client import Client
from nats.js import JetStreamContext
from nucliadb_protos.audit_pb2 import AuditField, AuditRequest
from nucliadb_protos.resources_pb2 import (
    TEXT,
    Classification,
    CloudFile,
    Entity,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    FileExtractedData,
    LargeComputedMetadataWrapper,
    Origin,
    Paragraph,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_ingest import SERVICE_NAME
from nucliadb_ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_indexing, get_storage

EXAMPLE_VECTOR = base64.b64decode(
    "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"
)


@pytest.mark.asyncio
async def test_ingest_messages_autocommit(
    local_files, gcs_storage: Storage, txn, cache, fake_node, processor, knowledgebox
):
    rid = str(uuid.uuid4())
    message1: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug="slug1",
        type=BrokerMessage.AUTOCOMMIT,
    )
    filename = f"{dirname(__file__)}/assets/file.png"
    cf1 = CloudFile(
        uri="file.png",
        source=CloudFile.Source.LOCAL,
        bucket_name="/ingest/assets",
        size=getsize(filename),
        content_type="image/png",
        filename="file.png",
    )
    message1.basic.icon = "text/plain"
    message1.basic.title = "Title Resource"
    message1.basic.summary = "Summary of document"
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
    local_files, gcs_storage: Storage, txn, cache, fake_node, processor, knowledgebox
):
    filename = f"{dirname(__file__)}/assets/resource.pb"
    with open(filename, "r") as f:
        data = base64.b64decode(f.read())
    message0: BrokerMessage = BrokerMessage()
    message0.ParseFromString(data)
    message0.kbid = knowledgebox
    message0.source = BrokerMessage.MessageSource.WRITER
    await processor.process(message=message0, seqid=1)

    filename = f"{dirname(__file__)}/assets/error.pb"
    with open(filename, "r") as f:
        data = base64.b64decode(f.read())
    message1: BrokerMessage = BrokerMessage()
    message1.ParseFromString(data)
    message1.kbid = knowledgebox
    message1.ClearField("field_vectors")
    message1.source = BrokerMessage.MessageSource.WRITER
    await processor.process(message=message1, seqid=2)

    kb_obj = KnowledgeBox(txn, gcs_storage, cache, kbid=knowledgebox)
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


def make_test_message(
    kbid: str, rid: str, filenames=[], message_type=BrokerMessage.AUTOCOMMIT
):
    message1: BrokerMessage = BrokerMessage(
        kbid=kbid,
        uuid=rid,
        slug="slug1",
        type=message_type,
    )
    for filename in filenames:
        file_path = f"{dirname(__file__)}/assets/file.png"
        cf1 = CloudFile(
            uri="file.png",
            source=CloudFile.Source.LOCAL,
            bucket_name="/ingest/assets",
            size=getsize(file_path),
            content_type="image/png",
            filename=f"{filename}.png",
        )
        message1.basic.icon = "text/plain"
        message1.basic.title = "Title Resource"
        message1.basic.summary = "Summary of document"
        message1.basic.thumbnail = "doc"
        message1.basic.layout = "default"
        message1.basic.metadata.language = "es"
        message1.basic.created.FromDatetime(datetime.now())
        message1.basic.modified.FromDatetime(datetime.now())
        message1.origin.source = Origin.Source.WEB
        message1.files[filename].file.CopyFrom(cf1)

    return message1


async def get_audit_messages(sub):
    msg = await sub.fetch(1)
    auditreq = AuditRequest()
    auditreq.ParseFromString(msg[0].data)
    return auditreq


@pytest.mark.asyncio
async def test_ingest_audit_stream(
    local_files,
    gcs_storage: Storage,
    txn,
    cache,
    fake_node,
    knowledgebox,
    stream_processor,
    stream_audit: StreamAuditStorage,
):
    from nucliadb_utils.settings import audit_settings

    # Prepare a test audit stream to receive our messages
    partition = stream_audit.get_partition(knowledgebox)
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
    message = make_test_message(knowledgebox, rid, ["file1"])

    await stream_processor.process(message=message, seqid=1)

    auditreq = await get_audit_messages(psub)
    assert auditreq.kbid == knowledgebox
    assert auditreq.rid == rid
    assert auditreq.storage_fields[0].size == message.files["file1"].file.size
    assert auditreq.storage_fields[0].action == AuditField.FieldAction.MODIFIED

    message.files.clear()
    fieldid = FieldID(field="file1", field_type=FieldType.FILE)
    message.delete_fields.append(fieldid)

    await stream_processor.process(message=message, seqid=2)

    auditreq = await get_audit_messages(psub)
    assert auditreq.kbid == knowledgebox
    assert auditreq.rid == rid
    assert auditreq.storage_fields[0].size == message.files["file1"].file.size
    assert auditreq.storage_fields[0].action == AuditField.FieldAction.DELETED

    await client.drain()
    await client.close()
