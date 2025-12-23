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
from unittest.mock import patch

import pytest
from httpx import AsyncClient
from nidx_protos.nodewriter_pb2 import VectorType

from nucliadb.common import datamanagers
from nucliadb.common.cluster.rollover import rollover_kb_index
from nucliadb.common.context import ApplicationContext
from nucliadb.common.maindb.utils import get_driver
from nucliadb.learning_proxy import (
    LearningConfiguration,
    ProxiedLearningConfigError,
    SemanticConfig,
    SimilarityFunction,
)
from nucliadb.purge import purge_kb_vectorsets
from nucliadb_models.search import KnowledgeboxCounters, KnowledgeboxSearchResults
from nucliadb_protos import utils_pb2
from nucliadb_protos.resources_pb2 import ExtractedVectorsWrapper, FieldType, Paragraph, Sentence
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    FieldID,
)
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.utilities import get_storage
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import mark_dirty, wait_for_sync
from tests.utils.vectorsets import add_vectorset

MODULE = "nucliadb.writer.api.v1.vectorsets"


@pytest.mark.deploy_modes("standalone")
async def test_vectorsets_crud(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox
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
            assert resp.status_code == 204, resp.text

        with patch(
            f"{MODULE}.learning_proxy.get_configuration",
            side_effect=[
                existing_lconfig,  # Initial configuration
                updated_lconfig,  # Configuration after adding the vectorset
                existing_lconfig,  # Initial configuration
            ],
        ):
            # Trying to add the vectorset that has just been deleted is not allowed until
            # purge has run
            resp = await nucliadb_writer.post(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            assert resp.status_code == 409, resp.text

            # Purge the vectorsets that are marked for deletion
            await purge_kb_vectorsets(get_driver(), await get_storage())

            # Add and delete the vectorset again
            resp = await nucliadb_writer.post(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            assert resp.status_code == 201, resp.text

            resp = await nucliadb_writer.delete(f"/kb/{kbid}/vectorsets/{vectorset_id}")
            assert resp.status_code == 204, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_learning_config_errors_are_proxied_correctly(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox
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


@pytest.mark.deploy_modes("standalone")
async def test_vectorset_migration(
    nucliadb_writer: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
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

    link_field = bmb.field_builder("link", FieldType.LINK)
    text = "Lionel Messi is a football player."
    link_field.with_extracted_text(text)
    link_field.with_extracted_paragraph_metadata(Paragraph(start=0, end=len(text)))
    link_vector = [1.0 / 1024**0.5 for _ in range(1024)]
    vectors = [
        utils_pb2.Vector(
            start=0,
            end=len(text),
            start_paragraph=0,
            end_paragraph=len(text),
            vector=link_vector,
        )
    ]
    link_field.with_extracted_vectors(vectors, vectorset="multilingual-2024-05-06")

    title_field = bmb.field_builder("title", FieldType.GENERIC)
    text = "link"
    title_field.with_extracted_text(text)
    title_field.with_extracted_paragraph_metadata(Paragraph(start=0, end=len(text)))
    title_vector = [1.0] + [0.0 for _ in range(1023)]
    vectors = [
        utils_pb2.Vector(
            start=0,
            end=len(text),
            start_paragraph=0,
            end_paragraph=len(text),
            vector=title_vector,
        )
    ]
    title_field.with_extracted_vectors(vectors, vectorset="multilingual-2024-05-06")
    bm = bmb.build()

    await inject_message(nucliadb_ingest_grpc, bm)

    await wait_for_sync()

    counters = await get_counters(nucliadb_reader, kbid)
    assert counters.resources == 1
    assert counters.paragraphs == 2  # one for the title and one for the link field
    assert counters.sentences == 2  # one for the title and one for the link field
    assert counters.fields == 2  # the title and the link field

    # Make a search and check that the document is found
    await _check_search(nucliadb_reader, kbid, query="football", vector=link_vector)

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
    link_vector_migrated = [0.0, 1.0] + [0.0 for _ in range(1022)]
    vector = utils_pb2.Vector(
        start=0,
        end=len(text),
        start_paragraph=0,
        end_paragraph=len(text),
    )
    vector.vector.extend(link_vector_migrated)
    ev.vectors.vectors.vectors.append(vector)
    bm2.field_vectors.append(ev)

    await inject_message(nucliadb_ingest_grpc, bm2)

    await wait_for_sync()

    # Check that the counters have been updated correctly: that is, only the sentences counter should have increased
    counters_after = await get_counters(nucliadb_reader, kbid)
    assert counters_after.sentences > counters.sentences
    assert counters_after.resources == counters.resources
    assert counters_after.paragraphs == counters.paragraphs
    assert counters_after.fields == counters.fields

    # Make a search with the new vectorset and check that the document is found
    await _check_search(
        nucliadb_reader,
        kbid,
        query="football",
        vectorset="en-2024-05-06",
        vector=link_vector_migrated,
    )

    # With the default vectorset the document should also be found
    await _check_search(
        nucliadb_reader,
        kbid,
        query="football",
        vectorset="multilingual-2024-05-06",
        vector=link_vector,
    )
    await _check_search(nucliadb_reader, kbid, query="football", vector=link_vector)

    # The title vector cannot be found in the new vectorset
    with pytest.raises(AssertionError):
        await _check_search(
            nucliadb_reader,
            kbid,
            query="link",
            vectorset="en-2024-05-06",
            vector=title_vector,
        )

    # Simulate that embeddings for the title field of this resource are also migrated
    # Ingest a new broker message as if it was coming from the migration
    bm3 = BrokerMessage(
        kbid=kbid,
        uuid=rid,
        type=BrokerMessage.MessageType.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    ev = ExtractedVectorsWrapper()
    ev.field.CopyFrom(FieldID(field_type=FieldType.GENERIC, field="title"))
    ev.vectorset_id = "en-2024-05-06"
    title_vector_migrated = [0.0, 0.0, 1.0] + [0.0 for _ in range(1021)]
    vector = utils_pb2.Vector(
        start=0,
        end=len(text),
        start_paragraph=0,
        end_paragraph=len(text),
    )
    vector.vector.extend(title_vector_migrated)
    ev.vectors.vectors.vectors.append(vector)
    bm3.field_vectors.append(ev)

    await inject_message(nucliadb_ingest_grpc, bm3)

    await wait_for_sync()

    # Make a search with the new vectorset and check that the document is found
    await _check_search(
        nucliadb_reader,
        kbid,
        query="football",
        vectorset="en-2024-05-06",
        vector=title_vector_migrated,
    )

    # With the default vectorset the document should also be found
    await _check_search(
        nucliadb_reader,
        kbid,
        query="football",
        vectorset="multilingual-2024-05-06",
        vector=title_vector,
    )
    await _check_search(nucliadb_reader, kbid, query="football", vector=title_vector)

    # Do a rollover and test again

    app_context = ApplicationContext()
    await app_context.initialize()

    await rollover_kb_index(app_context, kbid)

    await mark_dirty()
    await wait_for_sync()

    # Make a search with the new vectorset and check that the document is found
    await _check_search(
        nucliadb_reader,
        kbid,
        vectorset="en-2024-05-06",
        query="football",
        vector=link_vector_migrated,
    )

    # With the default vectorset the document should also be found
    await _check_search(
        nucliadb_reader,
        kbid,
        vectorset="multilingual-2024-05-06",
        query="football",
        vector=link_vector,
    )
    await _check_search(nucliadb_reader, kbid, query="football", vector=link_vector)

    await app_context.finalize()


async def get_counters(nucliadb_reader: AsyncClient, kbid: str) -> KnowledgeboxCounters:
    # We call counters endpoint to get the sentences count only
    resp = await nucliadb_reader.get(f"/kb/{kbid}/counters")
    assert resp.status_code == 200, resp.text
    counters = KnowledgeboxCounters.model_validate(resp.json())
    n_sentences = counters.sentences

    # We don't call /counters endpoint purposefully, as deletions are not guaranteed to be merged yet.
    # Instead, we do some searches.
    resp = await nucliadb_reader.get(f"/kb/{kbid}/search", params={"show": ["basic", "values"]})
    assert resp.status_code == 200, resp.text
    search = KnowledgeboxSearchResults.model_validate(resp.json())
    n_resources = len(search.resources)
    n_paragraphs = search.paragraphs.total  # type: ignore
    n_fields = sum(
        [
            len(resource.data.generics or {})
            + len(resource.data.files or {})
            + len(resource.data.links or {})
            + len(resource.data.texts or {})
            + len(resource.data.conversations or {})
            for resource in search.resources.values()
            if resource.data
        ]
    )
    # Update the counters object
    counters.resources = n_resources
    counters.paragraphs = n_paragraphs
    counters.sentences = n_sentences
    counters.fields = n_fields
    return counters


async def _check_search(
    nucliadb_reader: AsyncClient,
    kbid: str,
    query: str,
    vector: list[float],
    vectorset: str | None = None,
):
    # check semantic search
    payload = {
        "features": ["semantic"],
        "min_score": 0.9,
        "vector": vector,
    }
    if vectorset:
        payload["vectorset"] = vectorset
    resp = await nucliadb_reader.post(f"/kb/{kbid}/find", json=payload)
    assert resp.status_code == 200, resp.text
    results = resp.json()
    assert len(results["resources"]) == 1

    # check keyword search
    payload = {
        "query": query,
        "features": ["keyword"],
        "min_score": {"bm25": 0},
    }
    resp = await nucliadb_reader.post(f"/kb/{kbid}/find", json=payload)
    assert resp.status_code == 200, resp.text
    results = resp.json()
    assert len(results["resources"]) == 1


@pytest.mark.deploy_modes("standalone")
async def test_vectorset_migration_split_field(
    nucliadb_writer: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
):
    """Test workflow for adding a vectorset to an existing KB and ingesting
    partial updates (only for the new vectors).

    """
    old_vectorset_id = "multilingual-2024-05-06"
    new_vectorset_id = "en-2024-05-06"

    def vector_for_split(split_id):
        vector = [0.0 for _ in range(1024)]
        vector[split_id] = 1.0
        return vector

    # Create a KB
    resp = await nucliadb_writer_manager.post(
        "/kbs",
        json={
            "title": "migrationexamples",
            "description": "",
            "zone": "",
            "slug": "migrationexamples2",
            "learning_configuration": {
                "semantic_vector_similarity": "cosine",
                "anonymization_model": "disabled",
                "semantic_models": [old_vectorset_id],
                "semantic_model_configs": {
                    old_vectorset_id: {
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

    def _get_message(
        ident: str,
        who: str,
        to: list[str],
        text: str,
    ):
        return {
            "ident": ident,
            "who": who,
            "to": to,
            "content": {
                "text": text,
            },
        }

    # Create a conversation resource
    messages = [
        ("Alice", ["Bob"], "What is the plan of study for today?"),
        (
            "Bob",
            ["Alice"],
            "We will study the following topics: 1. Python basics 2. Data structures 3. Algorithms",
        ),
    ]
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "conversation",
            "conversations": {
                "conversation": {
                    "messages": [
                        _get_message(str(i + 1), src, dsts, text)  # split "0" is forbidden
                        for i, (src, dsts, text) in enumerate(messages)
                    ],
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
    conv_field = bmb.field_builder("conversation", FieldType.CONVERSATION)
    for idx, (_, _, text) in enumerate(messages):
        split = str(idx + 1)  # split "0" is forbidden
        conv_field.with_extracted_text(text, split=split)
        conv_field.with_extracted_paragraph_metadata(
            Paragraph(
                start=0,
                end=len(text),
                sentences=[
                    Sentence(
                        start=0,
                        end=len(text),
                    )
                ],
            ),
            split=split,
        )
        vectors = [
            utils_pb2.Vector(
                start=0,
                end=len(text),
                start_paragraph=0,
                end_paragraph=len(text),
                vector=vector_for_split(idx + 1),
            )
        ]
        conv_field.with_extracted_vectors(vectors, vectorset=old_vectorset_id, split=split)
    bm = bmb.build()

    await inject_message(nucliadb_ingest_grpc, bm)

    await wait_for_sync()

    counters = await get_counters(nucliadb_reader, kbid)
    assert counters.resources == 1
    assert counters.paragraphs == 3  # one for the title and one for each split of the conv field
    assert counters.sentences == 2  # only the vectors of each split of the conv field
    assert counters.fields == 2  # the title and the conv field

    # Make a search and check that the document is found
    await _check_search(nucliadb_reader, kbid, query="Python", vector=vector_for_split(1))

    # Now add a new vectorset
    resp = await add_vectorset(
        nucliadb_writer,
        kbid,
        new_vectorset_id,
        similarity=SimilarityFunction.COSINE,
        vector_dimension=1024,
    )
    assert resp.status_code == 201

    # Ingest a new broker message as if it was coming from the migration
    bm2 = BrokerMessage(
        kbid=kbid,
        uuid=rid,
        type=BrokerMessage.MessageType.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    field = FieldID(field_type=FieldType.CONVERSATION, field="conversation")

    # Right now, the semantic model migrator will add a field_vector item for each split
    for idx, (_, _, text) in enumerate(messages):
        split = str(idx + 1)
        ev = ExtractedVectorsWrapper()
        ev.field.CopyFrom(field)
        ev.vectorset_id = new_vectorset_id
        vector = utils_pb2.Vector(
            start=0,
            end=len(text),
            start_paragraph=0,
            end_paragraph=len(text),
        )
        vector.vector.extend(vector_for_split(idx + 101))
        ev.vectors.split_vectors[split].vectors.append(vector)
        bm2.field_vectors.append(ev)

    await inject_message(nucliadb_ingest_grpc, bm2)

    await wait_for_sync()

    # Check that the counters have been updated correctly: that is, only the sentences counter should have increased
    counters_after = await get_counters(nucliadb_reader, kbid)
    assert counters_after.sentences > counters.sentences
    assert counters_after.resources == counters.resources
    assert counters_after.paragraphs == counters.paragraphs
    assert counters_after.fields == counters.fields

    # Make a search with the new vectorset and check that the document is found
    await _check_search(
        nucliadb_reader,
        kbid,
        vectorset=new_vectorset_id,
        query="Python",
        vector=vector_for_split(101),
    )

    # With the default vectorset the document should also be found
    await _check_search(
        nucliadb_reader,
        kbid,
        vectorset=old_vectorset_id,
        query="Python",
        vector=vector_for_split(1),
    )
    await _check_search(nucliadb_reader, kbid, query="Python", vector=vector_for_split(1))

    # Do a rollover and test again
    app_context = ApplicationContext()
    await app_context.initialize()

    await rollover_kb_index(app_context, kbid)

    await mark_dirty()
    await wait_for_sync()

    # Make a search with the new vectorset and check that the document is found
    await _check_search(
        nucliadb_reader,
        kbid,
        vectorset=new_vectorset_id,
        query="Python",
        vector=vector_for_split(101),
    )

    # With the default vectorset the document should also be found
    await _check_search(
        nucliadb_reader,
        kbid,
        vectorset=old_vectorset_id,
        query="Python",
        vector=vector_for_split(1),
    )
    await _check_search(nucliadb_reader, kbid, query="Python", vector=vector_for_split(1))

    await app_context.finalize()
