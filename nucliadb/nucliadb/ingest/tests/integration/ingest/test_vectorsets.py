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
import random
import uuid

import pytest

from nucliadb.ingest import SERVICE_NAME
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.tests.fixtures import make_extracted_text
from nucliadb_protos import resources_pb2, utils_pb2, writer_pb2
from nucliadb_utils.utilities import get_indexing, get_storage


@pytest.mark.asyncio
async def test_ingest_broker_message_with_vectorsets(
    fake_node,
    storage,
    knowledgebox_ingest: str,
    processor: Processor,
):
    """Validate ingestion of a BrokerMessage containing vectors for the default
    vectors index and an additional vectorset.

    """
    kbid = knowledgebox_ingest
    rid = uuid.uuid4().hex
    slug = "my-resource"
    field_id = "my-text-field"
    vectorset_id = "fancy-multilang"
    body = "Lorem ipsum dolor sit amet..."
    default_vectorset_dimension = 10
    vectorset_dimension = 7

    bm = writer_pb2.BrokerMessage(
        kbid=kbid, uuid=rid, slug=slug, type=writer_pb2.BrokerMessage.AUTOCOMMIT
    )

    bm.texts[field_id].body = body

    bm.extracted_text.append(make_extracted_text(field_id, body))

    # default vectorset
    field_vectors = resources_pb2.ExtractedVectorsWrapper()
    field_vectors.field.field = field_id
    field_vectors.field.field_type = resources_pb2.FieldType.TEXT
    for i in range(0, 100, 10):
        field_vectors.vectors.vectors.vectors.append(
            utils_pb2.Vector(
                start=i,
                end=i + 10,
                vector=[random.random()] * default_vectorset_dimension,
            )
        )
    bm.field_vectors.append(field_vectors)

    # custom vectorset
    field_vectors = resources_pb2.ExtractedVectorsWrapper()
    field_vectors.field.field = field_id
    field_vectors.field.field_type = resources_pb2.FieldType.TEXT
    field_vectors.vectorset_id = vectorset_id
    for i in range(0, 100, 10):
        field_vectors.vectors.vectors.vectors.append(
            utils_pb2.Vector(
                start=i,
                end=i + 10,
                vector=[random.random()] * vectorset_dimension,
            )
        )
    bm.field_vectors.append(field_vectors)

    await processor.process(message=bm, seqid=1)

    index = get_indexing()
    storage = await get_storage(service_name=SERVICE_NAME)

    pb = await storage.get_indexing(index._calls[0][1])
    assert len(pb.paragraphs) == 1
    assert f"t/{field_id}" in pb.paragraphs
    field_paragraphs = pb.paragraphs[f"t/{field_id}"]
    for paragraph_id, paragraph in field_paragraphs.paragraphs.items():
        for vector_id, sentence in paragraph.sentences.items():
            assert len(sentence.vector) == default_vectorset_dimension

        assert len(paragraph.vectorsets_sentences) == 1
        assert vectorset_id in paragraph.vectorsets_sentences
        vectorset_sentences = paragraph.vectorsets_sentences[vectorset_id]
        for vector_id, sentence in vectorset_sentences.sentences.items():
            assert len(sentence.vector) == vectorset_dimension
