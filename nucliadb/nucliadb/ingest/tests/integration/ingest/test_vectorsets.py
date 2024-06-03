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

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest import SERVICE_NAME
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.tests.fixtures import make_extracted_text
from nucliadb_protos import (
    knowledgebox_pb2,
    noderesources_pb2,
    nodewriter_pb2,
    resources_pb2,
    utils_pb2,
    writer_pb2,
)
from nucliadb_utils.utilities import get_indexing, get_storage


@pytest.mark.asyncio
async def test_ingest_broker_message_with_vectorsets(
    fake_node,
    storage,
    knowledgebox_ingest: str,
    processor: Processor,
    maindb_driver: Driver,
):
    """Validate ingestion of a BrokerMessage containing vectors for the default
    vectors index and an additional vectorset.

    """
    kbid = knowledgebox_ingest
    rid = uuid.uuid4().hex
    field_id = "my-text-field"
    vectorset_id = "fancy-multilang"
    default_vectorset_dimension = 10
    vectorset_dimension = 7

    # HACK: add a vectorset directly in maindb so the ingestion founds it and
    # produces the correct brain
    async with datamanagers.with_transaction() as txn:
        await datamanagers.vectorsets.set(
            txn,
            kbid=kbid,
            config=knowledgebox_pb2.VectorSetConfig(
                vectorset_id=vectorset_id,
                vectorset_index_config=nodewriter_pb2.VectorIndexConfig(
                    vector_dimension=vectorset_dimension
                ),
            ),
        )
        await txn.commit()

    def validate_index_message(resource: noderesources_pb2.Resource):
        assert len(resource.paragraphs) == 1
        field_id = list(resource.paragraphs.keys())[0]
        field_paragraphs = resource.paragraphs[field_id]
        for paragraph_id, paragraph in field_paragraphs.paragraphs.items():
            for vector_id, sentence in paragraph.sentences.items():
                assert len(sentence.vector) == default_vectorset_dimension

            assert len(paragraph.vectorsets_sentences) == 1
            assert vectorset_id in paragraph.vectorsets_sentences
            vectorset_sentences = paragraph.vectorsets_sentences[vectorset_id]
            for vector_id, sentence in vectorset_sentences.sentences.items():
                assert len(sentence.vector) == vectorset_dimension

    index = get_indexing()
    storage = await get_storage(service_name=SERVICE_NAME)

    # process the broker message with vectorsets
    bm = create_broker_message_with_vectorset(
        kbid,
        rid,
        field_id,
        vectorset_id,
        default_vectorset_dimension=default_vectorset_dimension,
        vectorset_dimension=vectorset_dimension,
    )
    await processor.process(message=bm, seqid=1)

    pb = await storage.get_indexing(index._calls[0][1])
    validate_index_message(pb)

    # Generate a reindex to validate storage.
    #
    # A BrokerMessage with reindex=True will trigger a full brain generation
    # from the stored resource
    bm = writer_pb2.BrokerMessage(
        kbid=kbid, uuid=rid, type=writer_pb2.BrokerMessage.AUTOCOMMIT
    )
    bm.reindex = True

    await processor.process(message=bm, seqid=2)

    pb = await storage.get_indexing(index._calls[1][1])
    validate_index_message(pb)


def create_broker_message_with_vectorset(
    kbid: str,
    rid: str,
    field_id: str,
    vectorset_id: str,
    *,
    default_vectorset_dimension: int,
    vectorset_dimension: int,
):
    bm = writer_pb2.BrokerMessage(
        kbid=kbid, uuid=rid, type=writer_pb2.BrokerMessage.AUTOCOMMIT
    )

    body = "Lorem ipsum dolor sit amet..."
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
    return bm
