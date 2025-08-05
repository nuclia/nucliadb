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

import pytest
from httpx import AsyncClient

from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos.writer_pb2 import BrokerMessage, Error, Generator
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import broker_resource, inject_message


@pytest.mark.parametrize(
    "endpoint,expected_status,payload",
    [
        ("/kb/{kbid}/resource/{rid}/reprocess", 202, None),
        (
            "/kb/{kbid}/resource/{rid}/file/myfield/upload",
            201,
            {"content": b"this is a file field"},
        ),
    ],
)
@pytest.mark.deploy_modes("standalone")
async def test_endpoint_set_resource_status_to_pending(
    endpoint,
    expected_status,
    payload,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    """
    - Create a resource with a status PROCESSED
    - Call endpoint that modifies the resource
    - Check that the status is set to PENDING
    """
    # Create a resource, processing
    br = broker_resource(standalone_knowledgebox)
    br.texts["text"].CopyFrom(
        rpb.FieldText(body="This is my text field", format=rpb.FieldText.Format.PLAIN)
    )
    await inject_message(nucliadb_ingest_grpc, br)

    # Receive message from processor
    br.source = BrokerMessage.MessageSource.PROCESSOR
    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Hello!"
    etw.field.field = "text"
    etw.field.field_type = rpb.FieldType.TEXT
    br.extracted_text.append(etw)
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/resource/{br.uuid}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PROCESSED"

    kwargs = payload or {}
    resp = await nucliadb_writer.post(
        endpoint.format(kbid=standalone_knowledgebox, rid=br.uuid),
        **kwargs,
    )
    assert resp.status_code == expected_status

    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/resource/{br.uuid}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PENDING"


@pytest.mark.deploy_modes("standalone")
async def test_field_status_errors_processor(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    # Create a resource, processing
    br = broker_resource(standalone_knowledgebox)
    br.texts["my_text"].CopyFrom(
        rpb.FieldText(body="This is my text field", format=rpb.FieldText.Format.PLAIN)
    )
    br.texts["other_text"].CopyFrom(
        rpb.FieldText(body="This is my text field", format=rpb.FieldText.Format.PLAIN)
    )
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PENDING"
    assert resp_json["data"]["texts"]["my_text"]["status"] == "PENDING"
    assert resp_json["data"]["texts"]["other_text"]["status"] == "PENDING"

    # Receive message from processor with errors
    br.source = BrokerMessage.MessageSource.PROCESSOR

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Hello!"
    etw.field.field = "my_text"
    etw.field.field_type = rpb.FieldType.TEXT
    br.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Bye!"
    etw.field.field = "other_text"
    etw.field.field_type = rpb.FieldType.TEXT
    br.extracted_text.append(etw)

    g = Generator()
    g.processor.SetInParent()
    br.generated_by.append(g)
    br.errors.append(
        Error(
            field_type=rpb.FieldType.TEXT,
            field="my_text",
            error="Processor failed",
            code=Error.ErrorCode.EXTRACT,
        )
    )
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "ERROR"
    assert resp_json["data"]["texts"]["my_text"]["status"] == "ERROR"
    assert resp_json["data"]["texts"]["other_text"]["status"] == "PROCESSED"
    assert len(resp_json["data"]["texts"]["my_text"]["errors"]) == 1

    # Receive message from processor without errors, previous errors are cleared
    br.errors.pop()
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PROCESSED"
    assert resp_json["data"]["texts"]["my_text"]["status"] == "PROCESSED"
    assert resp_json["data"]["texts"]["other_text"]["status"] == "PROCESSED"
    assert "errors" not in resp_json["data"]["texts"]["my_text"]


@pytest.mark.deploy_modes("standalone")
async def test_field_status_warnings_processor(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    # Create a resource, processing
    br = broker_resource(standalone_knowledgebox)
    br.texts["my_text"].CopyFrom(
        rpb.FieldText(body="This is my text field", format=rpb.FieldText.Format.PLAIN)
    )
    br.texts["other_text"].CopyFrom(
        rpb.FieldText(body="This is my text field", format=rpb.FieldText.Format.PLAIN)
    )
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PENDING"
    assert resp_json["data"]["texts"]["my_text"]["status"] == "PENDING"
    assert resp_json["data"]["texts"]["other_text"]["status"] == "PENDING"

    # Receive message from processor with errors
    br.source = BrokerMessage.MessageSource.PROCESSOR

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Hello!"
    etw.field.field = "my_text"
    etw.field.field_type = rpb.FieldType.TEXT
    br.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Bye!"
    etw.field.field = "other_text"
    etw.field.field_type = rpb.FieldType.TEXT
    br.extracted_text.append(etw)

    g = Generator()
    g.processor.SetInParent()
    br.generated_by.append(g)
    br.errors.append(
        Error(
            field_type=rpb.FieldType.TEXT,
            field="my_text",
            error="Processor failed",
            code=Error.ErrorCode.EXTRACT,
            severity=Error.Severity.WARNING,
        )
    )
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PROCESSED"
    assert resp_json["data"]["texts"]["my_text"]["status"] == "PROCESSED"
    assert resp_json["data"]["texts"]["other_text"]["status"] == "PROCESSED"
    assert len(resp_json["data"]["texts"]["my_text"]["errors"]) == 1
    assert resp_json["data"]["texts"]["my_text"]["errors"][0]["severity"] == "WARNING"

    # Receive message from processor without errors, previous warnings are cleared
    br.errors.pop()
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PROCESSED"
    assert resp_json["data"]["texts"]["my_text"]["status"] == "PROCESSED"
    assert resp_json["data"]["texts"]["other_text"]["status"] == "PROCESSED"
    assert "errors" not in resp_json["data"]["texts"]["my_text"]


@pytest.mark.deploy_modes("standalone")
async def test_field_status_errors_data_augmentation(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    # Create a resource, processing
    br = broker_resource(standalone_knowledgebox)
    br.texts["text"].CopyFrom(
        rpb.FieldText(body="This is my text field", format=rpb.FieldText.Format.PLAIN)
    )
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PENDING"
    assert resp_json["data"]["generics"]["title"]["status"] == "PENDING"
    assert resp_json["data"]["texts"]["text"]["status"] == "PENDING"

    # Receive message from processor with success
    br.source = BrokerMessage.MessageSource.PROCESSOR
    g = Generator()
    g.processor.SetInParent()
    br.generated_by.append(g)
    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Hello!"
    etw.field.field = "text"
    etw.field.field_type = rpb.FieldType.TEXT
    br.extracted_text.append(etw)
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PROCESSED"
    assert resp_json["data"]["generics"]["title"]["status"] == "PROCESSED"
    assert resp_json["data"]["texts"]["text"]["status"] == "PROCESSED"

    # Receive message from data augmentation with errors
    br.errors.append(
        Error(
            field_type=rpb.FieldType.TEXT,
            field="text",
            error="Data augmentation failed",
            code=Error.ErrorCode.DATAAUGMENTATION,
        )
    )
    g = Generator()
    g.data_augmentation.SetInParent()
    br.generated_by.pop()
    br.generated_by.append(g)
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PROCESSED"
    assert resp_json["data"]["generics"]["title"]["status"] == "PROCESSED"
    assert resp_json["data"]["texts"]["text"]["status"] == "ERROR"
    assert len(resp_json["data"]["texts"]["text"]["errors"]) == 1

    # Receive message from data augmentation without errors
    br.errors.pop()
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{br.uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PROCESSED"
    assert resp_json["data"]["generics"]["title"]["status"] == "PROCESSED"
    assert resp_json["data"]["texts"]["text"]["status"] == "ERROR"
    assert len(resp_json["data"]["texts"]["text"]["errors"]) == 1


@pytest.mark.deploy_modes("standalone")
async def test_empty_resource_is_processed(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
):
    # Create a resource, processing
    br = broker_resource(standalone_knowledgebox)
    await inject_message(nucliadb_ingest_grpc, br)

    resp = await nucliadb_writer.post(f"/kb/{standalone_knowledgebox}/resources", json={})
    assert resp.status_code == 201
    resp_json = resp.json()

    uuid = resp_json["uuid"]
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/resource/{uuid}?show=basic&show=errors"
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PROCESSED"
