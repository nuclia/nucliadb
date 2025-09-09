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
from unittest.mock import patch

import pytest
from faker import Faker
from httpx import AsyncClient

from nucliadb.common.maindb.driver import Driver
from nucliadb.learning_proxy import LearningConfiguration
from nucliadb_models.search import SearchOptions
from nucliadb_protos import knowledgebox_pb2, resources_pb2, utils_pb2, writer_pb2
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder

fake = Faker()


@pytest.mark.deploy_modes("standalone")
async def test_matryoshka_embeddings(
    maindb_driver: Driver,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    learning_config,
    hosted_nucliadb,
):
    # Create a KB with matryoshka configuration (using ingest gRPC)

    kbid = str(uuid.uuid4())
    slug = "matryoshka-tests-kb"
    vector_dimension = 1024
    matryoshka_dimensions = [2048, 1024, 512]

    learning_config.get_configuration.return_value = LearningConfiguration(
        semantic_model="matryoshka",
        semantic_vector_similarity="dot",
        semantic_vector_size=matryoshka_dimensions[0],
        semantic_matryoshka_dims=matryoshka_dimensions,
    )

    new_kb_response = await nucliadb_ingest_grpc.NewKnowledgeBoxV2(  # type: ignore
        writer_pb2.NewKnowledgeBoxV2Request(
            kbid=kbid,
            slug=slug,
            vectorsets=[
                writer_pb2.NewKnowledgeBoxV2Request.VectorSet(
                    vectorset_id="my-semantic-model",
                    similarity=utils_pb2.VectorSimilarity.DOT,
                    # set vector dimension to the greatest matryoshka dimension, but
                    # we'll internally choose another one more convinient for nucliadb
                    vector_dimension=matryoshka_dimensions[0],
                    matryoshka_dimensions=matryoshka_dimensions,
                )
            ],
        )
    )
    assert new_kb_response.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    # Create a resource

    rslug = "matryoshka-resource"
    field_id = "my-text-field"
    title = "Matryoshka resource"
    body = fake.text(10000)

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": title,
            "slug": rslug,
            "texts": {"my-text-field": {"body": body}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Fake processing and inject vectors
    bmb = BrokerMessageBuilder(
        kbid=kbid, rid=rid, source=writer_pb2.BrokerMessage.MessageSource.PROCESSOR
    )
    bmb.with_title(title)

    text_field = bmb.field_builder(field_id, resources_pb2.FieldType.FILE)  # TODO: implement TEXT
    text_field.with_extracted_text(body)

    vectors = []
    for i in range(100):
        start = i * 100
        end = (i + 1) * 100
        vectors.append(
            utils_pb2.Vector(
                start=start,
                end=end,
                start_paragraph=start,
                end_paragraph=end,
                vector=[(i + 1)] * vector_dimension,
            )
        )
    text_field.with_extracted_vectors(vectors, vectorset="my-semantic-model")

    bm = bmb.build()
    await inject_message(nucliadb_ingest_grpc, bm)

    # Search

    from nucliadb.search import predict

    with patch.object(predict, "Q", [10.0] * vector_dimension):
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/search",
            params={
                "query": "matryoshka",
                "features": [SearchOptions.SEMANTIC.value],
                "min_score": 0.99999,
                "with_duplicates": True,
            },
        )
        assert resp.status_code == 200
        results = resp.json()

    # Validate results

    assert len(results["resources"]) == 1
    assert results["resources"][rid]["title"] == title
    # As all vectors normalized are the same, an entire page is returned and all
    # vectors have the same score
    assert len(results["sentences"]["results"]) == 20
    assert (
        len(set((semantic_result["score"] for semantic_result in results["sentences"]["results"]))) == 1
    )
