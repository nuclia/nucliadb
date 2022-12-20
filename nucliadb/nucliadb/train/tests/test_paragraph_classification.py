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

from typing import Callable
import aiohttp
from nucliadb.tests.utils import inject_message
from nucliadb.train import API_PREFIX
from nucliadb.train.api.v1.router import KB_PREFIX
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_protos.resources_pb2 import (
    Metadata,
    ParagraphAnnotation,
    UserFieldMetadata,
)
from nucliadb_protos.train_pb2 import (
    ParagraphClassificationBatch,
    TrainResponse,
    TrainSet,
    Type,
)
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
import pytest
from httpx import AsyncClient, Response, StreamConsumed
import asyncio


async def get_paragraph_classification_batch_from_response(
    response: aiohttp.ClientResponse,
) -> ParagraphClassificationBatch:
    try:
        header = await response.content.read(4)
        payload_size = int.from_bytes(header, byteorder="big", signed=False)
        payload = await response.content.read(payload_size)
        tr = TrainResponse()
        tr.ParseFromString(payload)
        assert tr.train == 6
        assert tr.test == 2
        while True:

            header = await response.content.read(4)
            payload_size = int.from_bytes(header, byteorder="big", signed=False)
            payload = await response.content.read(payload_size)
            pcb = ParagraphClassificationBatch()
            pcb.ParseFromString(payload)
            import pdb

            pdb.set_trace()
            return pcb
    except RuntimeError:
        pass


def broker_resource(knowledgebox: str) -> BrokerMessage:
    import uuid
    from datetime import datetime

    from nucliadb.ingest.tests.vectors import V1, V2, V3
    from nucliadb_protos import resources_pb2 as rpb

    rid = str(uuid.uuid4())
    slug = f"{rid}slug1"

    bm: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug=slug,
        type=BrokerMessage.AUTOCOMMIT,
    )

    bm.basic.icon = "text/plain"
    bm.basic.title = "Title Resource"
    bm.basic.summary = "Summary of document"
    bm.basic.thumbnail = "doc"
    bm.basic.layout = "default"
    bm.basic.metadata.useful = True
    bm.basic.metadata.status = Metadata.Status.PROCESSED
    bm.basic.metadata.language = "es"
    bm.basic.created.FromDatetime(datetime.now())
    bm.basic.modified.FromDatetime(datetime.now())
    bm.origin.source = rpb.Origin.Source.WEB

    c4 = rpb.Classification()
    c4.label = "label_user"
    c4.labelset = "labelset_resources"

    bm.basic.usermetadata.classifications.append(c4)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "My own text Ramon. This is great to be here. \n Where is my beer? Do you want to go shooping? This is a test!"
    etw.field.field = "file"
    etw.field.field_type = rpb.FieldType.FILE
    bm.extracted_text.append(etw)

    c3 = rpb.Classification()
    c3.label = "label_user"
    c3.labelset = "labelset_paragraphs"

    pa = ParagraphAnnotation()
    pa.classifications.append(c3)
    pa.key = "N_RID/f/file/47-64"  # Designed to be the RID at indexing time
    ufm = UserFieldMetadata()
    ufm.paragraphs.append(pa)

    pa = ParagraphAnnotation()
    pa.classifications.append(c3)
    pa.key = "N_RID/f/file/0-45"  # Designed to be the RID at indexing time
    ufm.paragraphs.append(pa)

    pa = ParagraphAnnotation()
    pa.classifications.append(c3)
    pa.key = "N_RID/f/file/65-93"  # Designed to be the RID at indexing time
    ufm.paragraphs.append(pa)

    pa = ParagraphAnnotation()
    pa.classifications.append(c3)
    pa.key = "N_RID/f/file/94-109"  # Designed to be the RID at indexing time
    ufm.paragraphs.append(pa)

    ufm.field.field = "file"
    ufm.field.field_type = rpb.FieldType.FILE
    bm.basic.fieldmetadata.append(ufm)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Summary of document"
    etw.field.field = "summary"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Title Resource"
    etw.field.field = "title"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    bm.files["file"].added.FromDatetime(datetime.now())
    bm.files["file"].file.source = rpb.CloudFile.Source.EXTERNAL

    c1 = rpb.Classification()
    c1.label = "label_machine"
    c1.labelset = "labelset_paragraphs"

    c2 = rpb.Classification()
    c2.label = "label_machine"
    c2.labelset = "labelset_resources"

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "file"
    fcm.field.field_type = rpb.FieldType.FILE
    p1 = rpb.Paragraph(
        start=0,
        end=45,
    )
    p1.classifications.append(c1)
    p2 = rpb.Paragraph(
        start=47,
        end=64,
    )
    p2.classifications.append(c1)

    p3 = rpb.Paragraph(
        start=65,
        end=93,
    )
    p3.classifications.append(c1)

    p4 = rpb.Paragraph(
        start=94,
        end=109,
    )
    p4.classifications.append(c1)

    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.paragraphs.append(p2)
    fcm.metadata.metadata.paragraphs.append(p3)
    fcm.metadata.metadata.paragraphs.append(p4)
    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.ner["Ramon"] = "PERSON"

    fcm.metadata.metadata.classifications.append(c2)
    bm.field_metadata.append(fcm)

    ev = rpb.ExtractedVectorsWrapper()
    ev.field.field = "file"
    ev.field.field_type = rpb.FieldType.FILE

    bm.source = BrokerMessage.MessageSource.WRITER
    return bm


async def inject_resource_with_paragraph_labels(knowledgebox, writer):
    bm = broker_resource(knowledgebox)
    await inject_message(writer, bm)
    return bm.uuid


@pytest.mark.asyncio
async def test_generator_paragraph_classification(
    train_rest_api: aiohttp.ClientSession, knowledgebox: str, nucliadb_grpc: WriterStub
):

    await inject_resource_with_paragraph_labels(knowledgebox, nucliadb_grpc)
    await asyncio.sleep(0.1)
    async with train_rest_api.get(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{knowledgebox}/trainset"
    ) as partitions:
        assert partitions.status == 200
        data = await partitions.json()
        assert len(data["partitions"]) == 1
        partition_id = data["partitions"][0]

    trainset = TrainSet()
    trainset.type == Type.PARAGRAPH_CLASSIFICATION
    trainset.batch_size = 2
    trainset.filter.labels.append("/l/labelset_paragraphs")
    async with train_rest_api.post(
        f"/{API_PREFIX}/v1/{KB_PREFIX}/{knowledgebox}/trainset/{partition_id}",
        data=trainset.SerializeToString(),
    ) as response:

        assert response.status == 200
        pcb = await get_paragraph_classification_batch_from_response(response)
        assert pcb.data
        import pdb

        pdb.set_trace()
