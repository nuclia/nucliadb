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
from typing import Optional
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.cluster.rollover import rollover_kb_index
from nucliadb.common.context import ApplicationContext
from nucliadb.learning_proxy import (
    LearningConfiguration,
    ProxiedLearningConfigError,
    SemanticConfig,
    SimilarityFunction,
)
from nucliadb_protos import utils_pb2
from nucliadb_protos.nodewriter_pb2 import VectorType
from nucliadb_protos.resources_pb2 import ExtractedVectorsWrapper, FieldType, Paragraph
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    FieldID,
)
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder, FieldBuilder
from tests.utils.dirty_index import mark_dirty, wait_for_sync
from tests.utils.vectorsets import add_vectorset

MODULE = "nucliadb.writer.api.v1.vectorsets"


@pytest.mark.deploy_modes("standalone")
async def test_vectorsets_crud(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    kbid = knowledgebox
    vectorset_id = "en-2024-04-24"
    existing_lconfig = LearningConfiguration(
        semantic_model="multilingual",
        semantic_threshold=0.5,
        semantic_vector_similarity=SimilarityFunction.COSINE.name,
        semantic_models=["multilingual"],
        semantic_model_configs={
            "multilingual": SemanticConfig(
                similarity=SimilarityFunction.COSINE,
                size=1024,
                threshold=0.5,
            )
        },
    )
    updated_lconfig = LearningConfiguration(
        semantic_model="multilingual",
        semantic_threshold=0.5,
        semantic_vector_similarity=SimilarityFunction.COSINE.name,
        semantic_models=["multilingual", vectorset_id],
        semantic_model_configs={
            "multilingual": SemanticConfig(
                similarity=SimilarityFunction.COSINE,
                size=1024,
                threshold=0.5,
            ),
            vectorset_id: SemanticConfig(
                similarity=SimilarityFunction.COSINE,
                size=1024,
                threshold=0.5,
                matryoshka_dims=[1024, 512, 256, 128],
            ),
        },
    )

    with patch(f"{MODULE}.learning_proxy.update_configuration", return_value=None):
        with patch(
            f"{MODULE}.learning_proxy.get_configuration",
            side_effect=[
                existing_lconfig,  # Initial configuration
                updated_lconfig,  # Configuration after adding the vectorset
            ],
        ):
            # Add the vectorset
            resp = await nucliadb_writer.post(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            assert resp.status_code == 201, resp.text

            # Check that the vectorset has been created with the correct configuration
            resp = await nucliadb_reader.get(f"/kb/{kbid}/vectorsets")
            assert resp.status_code == 200
            body = resp.json()
            assert len(body["vectorsets"]) == 2
            assert {"id": "multilingual"} in body["vectorsets"]
            assert {"id": vectorset_id} in body["vectorsets"]

            async with datamanagers.with_ro_transaction() as txn:
                vs = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset_id)
                assert vs is not None
                assert vs.vectorset_id == vectorset_id
                assert vs.vectorset_index_config.vector_type == VectorType.DENSE_F32
                assert vs.vectorset_index_config.similarity == utils_pb2.VectorSimilarity.COSINE
                assert vs.vectorset_index_config.vector_dimension == 1024
                assert vs.matryoshka_dimensions == [1024, 512, 256, 128]

        with patch(
            f"{MODULE}.learning_proxy.get_configuration",
            side_effect=[
                updated_lconfig,  # Configuration right before deleting the vectorset
            ],
        ):
            # Delete the vectorset
            resp = await nucliadb_writer.delete(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            assert resp.status_code == 204, resp.text

            # Check that the vectorset has been deleted
            resp = await nucliadb_reader.get(f"/kb/{kbid}/vectorsets")
            assert resp.status_code == 200
            body = resp.json()
            assert len(body["vectorsets"]) == 1
            assert {"id": "multilingual"} in body["vectorsets"]

            async with datamanagers.with_ro_transaction() as txn:
                vs = await datamanagers.vectorsets.get(txn, kbid=kbid, vectorset_id=vectorset_id)
                assert vs is None

        with patch(
            f"{MODULE}.learning_proxy.get_configuration",
            side_effect=[
                existing_lconfig,  # initial configuration again
            ],
        ):
            # Deleting your last vectorset is not allowed
            resp = await nucliadb_writer.delete(f"/kb/{kbid}/vectorsets/multilingual")
            assert resp.status_code == 409, resp.text
            assert "Deletion of your last vectorset is not allowed" in resp.json()["detail"]

        with patch(
            f"{MODULE}.learning_proxy.get_configuration",
            side_effect=[
                existing_lconfig,  # initial configuration again
            ],
        ):
            # But deleting twice is okay
            resp = await nucliadb_writer.delete(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            # XXX: however, we get the same error as before due to our lazy
            # check strategy. This shuold be a 200
            assert resp.status_code == 409, resp.text

        with patch(
            f"{MODULE}.learning_proxy.get_configuration",
            side_effect=[
                existing_lconfig,  # Initial configuration
                updated_lconfig,  # Configuration after adding the vectorset
                existing_lconfig,  # Initial configuration
            ],
        ):
            # Add and delete the vectorset again
            resp = await nucliadb_writer.post(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            assert resp.status_code == 201, resp.text
            resp = await nucliadb_writer.delete(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            assert resp.status_code == 204, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_learning_config_errors_are_proxied_correctly(
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    kbid = knowledgebox
    with patch(
        f"{MODULE}.learning_proxy.get_configuration",
        side_effect=ProxiedLearningConfigError(
            status_code=500, content="Learning Internal Server Error"
        ),
    ):
        resp = await nucliadb_writer.post(f"/kb/{kbid}/vectorsets/foo")
        assert resp.status_code == 500
        assert resp.json() == {"detail": "Learning Internal Server Error"}

        resp = await nucliadb_writer.delete(f"/kb/{kbid}/vectorsets/foo")
        assert resp.status_code == 500
        assert resp.json() == {"detail": "Learning Internal Server Error"}


@pytest.mark.parametrize("bwc_with_default_vectorset", [True, False])
@pytest.mark.deploy_modes("standalone")
async def test_vectorset_migration(
    nucliadb_writer: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    bwc_with_default_vectorset: bool,
):
    """Test workflow for adding a vectorset to an existing KB and ingesting
    partial updates (only for the new vectors).

    """

    # Create a KB
    resp = await nucliadb_writer_manager.post(
        "/kbs",
        json={
            "title": "migrationexamples",
            "description": "",
            "zone": "",
            "slug": "migrationexamples",
            "learning_configuration": {
                "semantic_vector_similarity": "cosine",
                "anonymization_model": "disabled",
                "semantic_models": ["multilingual-2024-05-06"],
                "semantic_model_configs": {
                    "multilingual-2024-05-06": {
                        "similarity": 0,
                        "size": 1024,
                        "threshold": 0.5,
                    }
                },
            },
        },
    )
    assert resp.status_code == 201, resp.text
    kbid = resp.json()["uuid"]

    # Create a link resource
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "link",
            "description": "link",
            "links": {
                "link": {
                    "uri": "https://en.wikipedia.org/wiki/Lionel_Messi",
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    # Ingest a processing broker message
    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )

    link_field = FieldBuilder("link", FieldType.LINK)
    text = "Lionel Messi is a football player."
    link_field.with_extracted_text(text)
    link_field.with_extracted_paragraph_metadata(Paragraph(start=0, end=len(text)))
    vectors = [
        utils_pb2.Vector(
            start=0,
            end=len(text),
            start_paragraph=0,
            end_paragraph=len(text),
            vector=[1.0 for _ in range(1024)],
        )
    ]
    link_field.with_extracted_vectors(vectors, vectorset="multilingual-2024-05-06")

    bmb.add_field_builder(link_field)
    bm = bmb.build()

    await inject_message(nucliadb_ingest_grpc, bm)

    # Make a search and check that the document is found
    await _check_search(nucliadb_reader, kbid)

    # Now add a new vectorset
    vectorset_id = "en-2024-05-06"
    resp = await add_vectorset(
        nucliadb_writer, kbid, vectorset_id, similarity=SimilarityFunction.COSINE, vector_dimension=1024
    )
    assert resp.status_code == 201

    # Ingest a new broker message as if it was coming from the migration
    bm2 = BrokerMessage(
        kbid=kbid,
        uuid=rid,
        type=BrokerMessage.MessageType.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    field = FieldID(field_type=FieldType.LINK, field="link")

    ev = ExtractedVectorsWrapper()
    ev.field.CopyFrom(field)
    ev.vectorset_id = "en-2024-05-06"
    vector = utils_pb2.Vector(
        start=0,
        end=len(text),
        start_paragraph=0,
        end_paragraph=len(text),
    )
    vector.vector.extend([2.0 for _ in range(1024)])
    ev.vectors.vectors.vectors.append(vector)
    bm2.field_vectors.append(ev)

    await inject_message(nucliadb_ingest_grpc, bm2)

    # Make a search with the new vectorset and check that the document is found
    await _check_search(nucliadb_reader, kbid, vectorset="en-2024-05-06")

    # With the default vectorset the document should also be found
    await _check_search(nucliadb_reader, kbid, vectorset="multilingual-2024-05-06")
    await _check_search(nucliadb_reader, kbid)

    # Do a rollover and test again

    app_context = ApplicationContext()
    await app_context.initialize()
    # await app_context.finalize()

    await rollover_kb_index(app_context, kbid)
    await mark_dirty()
    await wait_for_sync()

    # Make a search with the new vectorset and check that the document is found
    await _check_search(nucliadb_reader, kbid, vectorset="en-2024-05-06")

    # With the default vectorset the document should also be found
    await _check_search(nucliadb_reader, kbid, vectorset="multilingual-2024-05-06")
    await _check_search(nucliadb_reader, kbid)


async def _check_search(nucliadb_reader: AsyncClient, kbid: str, vectorset: Optional[str] = None):
    # check semantic search
    payload = {
        "features": ["semantic"],
        "min_score": -1,
        "vector": [1.0 for _ in range(1024)],
    }
    if vectorset:
        payload["vectorset"] = vectorset
    resp = await nucliadb_reader.post(f"/kb/{kbid}/find", json=payload)
    assert resp.status_code == 200, resp.text
    results = resp.json()
    assert len(results["resources"]) == 1

    # check keyword search
    payload = {
        "query": "football",
        "features": ["keyword"],
        "min_score": {"bm25": 0},
    }
    resp = await nucliadb_reader.post(f"/kb/{kbid}/find", json=payload)
    assert resp.status_code == 200, resp.text
    results = resp.json()
    assert len(results["resources"]) == 1
