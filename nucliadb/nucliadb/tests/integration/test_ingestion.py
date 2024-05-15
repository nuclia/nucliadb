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
from datetime import datetime
from unittest import mock

import pytest
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.tests.utils import inject_message
from nucliadb_protos import resources_pb2 as rpb


def broker_resource_writer(knowledgebox: str, rid: str) -> BrokerMessage:
    slug = f"{rid}slug1"
    bm: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug=slug,
        type=BrokerMessage.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.WRITER,
    )
    # Audit
    bm.audit.user = "foo"
    bm.audit.when.FromDatetime(datetime.now())
    bm.audit.origin = "127.0.1.27"
    # Basic
    bm.basic.icon = "application/stf-link"
    bm.basic.title = "https://en.wikipedia.org/wiki/Lionel_Messi"
    bm.basic.metadata.status = rpb.Metadata.Status.PENDING
    bm.basic.metadata.useful = True
    bm.basic.metadata.language = "es"
    bm.basic.created.FromDatetime(datetime.now())
    bm.basic.modified.FromDatetime(datetime.now())
    # Origin
    bm.origin.source = rpb.Origin.Source.API
    bm.origin.url = "https://en.wikipedia.org/wiki/Lionel_Messi"
    # Links
    bm.links["link"].added.FromDatetime(datetime.now())
    bm.links["link"].uri = "https://en.wikipedia.org/wiki/Lionel_Messi"
    # Extracted text
    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "https://en.wikipedia.org/wiki/Lionel_Messi"
    etw.field.field = "title"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)
    # Field metadata
    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "title"
    fcm.field.field_type = rpb.FieldType.GENERIC
    p1 = rpb.Paragraph(
        end=42,
        kind=rpb.Paragraph.TypeParagraph.TITLE,
    )
    fcm.metadata.metadata.paragraphs.append(p1)
    bm.field_metadata.append(fcm)
    return bm


def broker_resource_processor(knowledgebox: str, rid: str) -> BrokerMessage:
    bm: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        type=BrokerMessage.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.PROCESSOR,
        processing_id="foobar",
    )
    bm.done_time.FromDatetime(datetime.now())

    # Extracted text
    etw = rpb.ExtractedTextWrapper()
    etw.field.field = "link"
    etw.field.field_type = rpb.FieldType.LINK
    etw.body.text = "Lionel Messi - Wikipedia"
    bm.extracted_text.append(etw)
    etw = rpb.ExtractedTextWrapper()
    etw.field.field = "title"
    etw.field.field_type = rpb.FieldType.GENERIC
    etw.body.text = "https://en.wikipedia.org/wiki/Lionel_Messi"

    # Field metadata: link
    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "link"
    fcm.field.field_type = rpb.FieldType.LINK
    paragraph = rpb.Paragraph(end=23)
    sentence = rpb.Sentence(end=23)
    paragraph.sentences.append(sentence)
    paragraph.page.page_with_visual = True
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.paragraphs.append(paragraph)
    bm.field_metadata.append(fcm)

    # Field metadata: title
    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "title"
    fcm.field.field_type = rpb.FieldType.GENERIC
    paragraph = rpb.Paragraph(end=42)
    sentence = rpb.Sentence(end=42)
    paragraph.sentences.append(sentence)
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.paragraphs.append(paragraph)
    bm.field_metadata.append(fcm)

    # Field vectors
    evw = rpb.ExtractedVectorsWrapper()
    evw.field.field = "link"
    evw.field.field_type = rpb.FieldType.LINK
    vector = Vector(
        start=0,
        end=23,
        start_paragraph=0,
        end_paragraph=23,
    )
    vector.vector.extend([0.1, 0.2, 0.3])
    evw.vectors.vectors.vectors.append(vector)
    bm.field_vectors.append(evw)
    return bm


def broker_resource_writer_update(knowledgebox: str, rid: str) -> BrokerMessage:
    # Change the link to point to Cristiano Ronaldo
    bm: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        type=BrokerMessage.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.WRITER,
    )
    # Audit
    bm.audit.user = "foo"
    bm.audit.when.FromDatetime(datetime.now())
    bm.audit.origin = "127.0.1.27"
    # Basic
    bm.basic.metadata.status = rpb.Metadata.Status.PENDING
    bm.basic.metadata.useful = True
    bm.basic.metadata.language = "es"
    bm.basic.created.FromDatetime(datetime.now())
    bm.basic.modified.FromDatetime(datetime.now())
    # Links
    bm.links["link"].added.FromDatetime(datetime.now())
    bm.links["link"].uri = "https://en.wikipedia.org/wiki/Cristiano_Ronaldo"
    return bm


def broker_resource_processor_update(knowledgebox: str, rid: str) -> BrokerMessage:
    bm: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        type=BrokerMessage.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.PROCESSOR,
        processing_id="foobar",
    )
    bm.done_time.FromDatetime(datetime.now())

    # Extracted text
    etw = rpb.ExtractedTextWrapper()
    etw.field.field = "link"
    etw.field.field_type = rpb.FieldType.LINK
    etw.body.text = "Cristiano Ronaldo - Wikipedia"
    bm.extracted_text.append(etw)

    # Field metadata: link
    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "link"
    fcm.field.field_type = rpb.FieldType.LINK
    paragraph = rpb.Paragraph(end=29)
    sentence = rpb.Sentence(end=29)
    paragraph.sentences.append(sentence)
    paragraph.page.page_with_visual = True
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.paragraphs.append(paragraph)
    bm.field_metadata.append(fcm)

    # Field vectors
    evw = rpb.ExtractedVectorsWrapper()
    evw.field.field = "link"
    evw.field.field_type = rpb.FieldType.LINK
    vector = Vector(
        start=0,
        end=29,
        start_paragraph=0,
        end_paragraph=29,
    )
    vector.vector.extend([0.5, 0.6, 0.7])
    evw.vectors.vectors.vectors.append(vector)
    bm.field_vectors.append(evw)
    return bm


@pytest.fixture(scope="function")
def add_resource_mock():
    with mock.patch(
        "nucliadb.common.cluster.manager.StandaloneKBShardManager.add_resource"
    ) as add_resource_mock:
        yield add_resource_mock


def get_brains(add_resource_mock):
    return [call_args[0][1] for call_args in add_resource_mock.call_args_list]


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_ingestion(
    add_resource_mock,
    nucliadb_grpc,
    knowledgebox,
):
    rid = str(uuid.uuid4())
    # Create a new resource first
    bm_writer_create = broker_resource_writer(knowledgebox, rid)
    bm_processor_create = broker_resource_processor(knowledgebox, rid)
    await inject_message(nucliadb_grpc, bm_writer_create)
    await inject_message(nucliadb_grpc, bm_processor_create)

    brain_writer, brain_processor = get_brains(add_resource_mock)

    # Check that sentences to delete are empty
    assert len(brain_writer.sentences_to_delete) == 0
    assert len(brain_processor.sentences_to_delete) == 0

    # Update the resource
    bm_writer_update = broker_resource_writer_update(knowledgebox, rid)
    bm_processor_update = broker_resource_processor_update(knowledgebox, rid)
    await inject_message(nucliadb_grpc, bm_writer_update)
    await inject_message(nucliadb_grpc, bm_processor_update)

    brain_writer, brain_processor = get_brains(add_resource_mock)[-2:]

    # Check that sentences to delete are not empty
    assert len(brain_writer.sentences_to_delete) == 0
    assert len(brain_processor.sentences_to_delete) > 0
