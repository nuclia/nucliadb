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
import unittest
from datetime import datetime
from unittest import mock
from unittest.mock import call
from uuid import uuid4

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos.knowledgebox_pb2 import (
    CreateExternalIndexProviderMetadata,
    CreatePineconeConfig,
    ExternalIndexProviderType,
    KnowledgeBoxConfig,
    KnowledgeBoxID,
    KnowledgeBoxNew,
    KnowledgeBoxResponseStatus,
    NewKnowledgeBoxResponse,
    PineconeServerlessCloud,
)
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_protos.writer_pb2 import BrokerMessage, NewKnowledgeBoxV2Request, OpStatusWriter
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.aiopynecone.models import QueryResponse
from nucliadb_utils.utilities import get_endecryptor

PINECONE_MODULE = "nucliadb.common.external_index_providers.pinecone"


@pytest.fixture()
def data_plane():
    mocked = mock.MagicMock()
    mocked.delete_by_id_prefix = mock.AsyncMock(return_value=None)
    mocked.upsert_in_batches = mock.AsyncMock(return_value=None)
    mocked.query = mock.AsyncMock(return_value=QueryResponse(matches=[]))
    return mocked


@pytest.fixture()
def control_plane():
    mocked = mock.MagicMock()
    mocked.create_index = mock.AsyncMock(return_value="pinecone-host")
    mocked.delete_index = mock.AsyncMock(return_value=None)
    return mocked


@pytest.fixture(autouse=True)
def mock_pinecone_client(data_plane, control_plane):
    session_mock = mock.MagicMock()
    session_mock.control_plane.return_value = control_plane
    session_mock.data_plane.return_value = data_plane
    with (
        unittest.mock.patch(f"{PINECONE_MODULE}.get_pinecone", return_value=session_mock),
    ):
        yield session_mock


@pytest.fixture(autouse=True)
def hosted_nucliadb():
    with unittest.mock.patch("nucliadb.ingest.service.writer.is_onprem_nucliadb", return_value=False):
        yield


@pytest.mark.asyncio
async def test_kb_creation_old(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    control_plane,
):
    """
    This tests the deprecated method for creating kbs on a hosted nucliadb that
    uses the nucliadb_grpc.NewKnowledgeBox method.
    """
    expected_index_name = "nuclia-someuuid"
    with mock.patch(
        f"{PINECONE_MODULE}.PineconeIndexManager.get_index_name", return_value=expected_index_name
    ):
        kbid = str(uuid4())
        slug = "pinecone-testing-old"
        config = KnowledgeBoxConfig(
            title="Pinecone test",
            slug=slug,
        )
        request = KnowledgeBoxNew(
            forceuuid=kbid,
            slug=slug,
            config=config,
            vector_dimension=128,
            similarity=VectorSimilarity.DOT,
            external_index_provider=CreateExternalIndexProviderMetadata(
                type=ExternalIndexProviderType.PINECONE,
                pinecone_config=CreatePineconeConfig(
                    api_key="my-pinecone-api-key",
                    serverless_cloud=PineconeServerlessCloud.AWS_US_EAST_1,
                ),
            ),
        )

        # Creating a knowledge box should create a Pinecone index
        response: NewKnowledgeBoxResponse = await nucliadb_grpc.NewKnowledgeBox(request, timeout=None)  # type: ignore
        assert response.status == KnowledgeBoxResponseStatus.OK

        control_plane.create_index.assert_awaited_once_with(
            name=expected_index_name,
            dimension=128,
            metric="dotproduct",
            serverless_cloud={"cloud": "aws", "region": "us-east-1"},
        )

        # Check that external index provider metadata was properly stored
        async with datamanagers.with_ro_transaction() as txn:
            external_index_provider = await datamanagers.kb.get_external_index_provider_metadata(
                txn, kbid=kbid
            )
            assert external_index_provider is not None
            assert external_index_provider.type == ExternalIndexProviderType.PINECONE
            # Check that the API key was encrypted
            encrypted_api_key = external_index_provider.pinecone_config.encrypted_api_key
            endecryptor = get_endecryptor()
            decrypted_api_key = endecryptor.decrypt(encrypted_api_key)
            assert decrypted_api_key == "my-pinecone-api-key"

            # Check that the rest of the config was stored
            vectorset_id = "__default__"
            assert (
                external_index_provider.pinecone_config.indexes[vectorset_id].index_name
                == expected_index_name
            )
            assert (
                external_index_provider.pinecone_config.indexes[vectorset_id].index_host
                == "pinecone-host"
            )
            assert external_index_provider.pinecone_config.indexes[vectorset_id].vector_dimension == 128

        # Deleting a knowledge box should delete the Pinecone index
        response = await nucliadb_grpc.DeleteKnowledgeBox(
            KnowledgeBoxID(slug=slug, uuid=kbid), timeout=None
        )  # type: ignore
        assert response.status == KnowledgeBoxResponseStatus.OK

        control_plane.delete_index.assert_awaited_once_with(name=expected_index_name)


@pytest.mark.asyncio
async def test_kb_creation_new(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    control_plane,
):
    """
    This tests the new method for creating kbs on a hosted nucliadb that
    uses the nucliadb_grpc.NewKnowledgeBoxV2 method.
    """
    expected_index_names = ["nuclia-someuuid1", "nuclia-someuuid2"]
    with mock.patch(
        f"{PINECONE_MODULE}.PineconeIndexManager.get_index_name", side_effect=expected_index_names
    ):
        kbid = str(uuid4())
        slug = "pinecone-testing-new"
        request = NewKnowledgeBoxV2Request(
            kbid=kbid,
            slug=slug,
            title="Pinecone test",
            description="Description",
            external_index_provider=CreateExternalIndexProviderMetadata(
                type=ExternalIndexProviderType.PINECONE,
                pinecone_config=CreatePineconeConfig(
                    api_key="my-pinecone-api-key",
                    serverless_cloud=PineconeServerlessCloud.AWS_US_EAST_1,
                ),
            ),
        )
        multilingual = "multilingual-2024-01-01"
        request.vectorsets.append(
            NewKnowledgeBoxV2Request.VectorSet(
                vectorset_id=multilingual,
                similarity=VectorSimilarity.DOT,
                vector_dimension=128,
            )
        )
        english = "english-2022-01-01"
        request.vectorsets.append(
            NewKnowledgeBoxV2Request.VectorSet(
                vectorset_id=english,
                similarity=VectorSimilarity.COSINE,
                vector_dimension=3,
            )
        )

        # Creating a knowledge with 2 vectorsets box should create two Pinecone indexes
        response: NewKnowledgeBoxResponse = await nucliadb_grpc.NewKnowledgeBoxV2(request, timeout=None)  # type: ignore
        assert response.status == KnowledgeBoxResponseStatus.OK

        # Should create an index for every vectorset
        control_plane.create_index.assert_has_calls(
            [
                call(
                    name=expected_index_names[0],
                    dimension=128,
                    metric="dotproduct",
                    serverless_cloud={"cloud": "aws", "region": "us-east-1"},
                ),
                call(
                    name=expected_index_names[1],
                    dimension=3,
                    metric="cosine",
                    serverless_cloud={"cloud": "aws", "region": "us-east-1"},
                ),
            ]
        )

        # Check that external index provider metadata was properly stored
        async with datamanagers.with_ro_transaction() as txn:
            external_index_provider = await datamanagers.kb.get_external_index_provider_metadata(
                txn, kbid=kbid
            )
            assert external_index_provider is not None
            assert external_index_provider.type == ExternalIndexProviderType.PINECONE
            # Check that the API key was encrypted
            encrypted_api_key = external_index_provider.pinecone_config.encrypted_api_key
            endecryptor = get_endecryptor()
            decrypted_api_key = endecryptor.decrypt(encrypted_api_key)
            assert decrypted_api_key == "my-pinecone-api-key"

            # Check that the rest of the config was stored
            pinecone_config = external_index_provider.pinecone_config
            assert pinecone_config.indexes[multilingual].index_name == expected_index_names[0]
            assert pinecone_config.indexes[multilingual].index_host == "pinecone-host"
            assert pinecone_config.indexes[multilingual].vector_dimension == 128
            assert pinecone_config.indexes[english].index_name == expected_index_names[1]
            assert pinecone_config.indexes[english].index_host == "pinecone-host"
            assert pinecone_config.indexes[english].vector_dimension == 3

        # Deleting a knowledge box should delete the Pinecone index
        response = await nucliadb_grpc.DeleteKnowledgeBox(
            KnowledgeBoxID(slug=slug, uuid=kbid), timeout=None
        )  # type: ignore
        assert response.status == KnowledgeBoxResponseStatus.OK

        # Test that deletes all external indexes
        deleted_index_names = set()
        deleted_index_names.add(control_plane.delete_index.call_args_list[0][1]["name"])
        deleted_index_names.add(control_plane.delete_index.call_args_list[1][1]["name"])
        assert deleted_index_names == set(expected_index_names)


async def test_get_kb(
    nucliadb_reader: AsyncClient,
    pinecone_knowledgebox: str,
):
    kbid = pinecone_knowledgebox

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}",
    )
    assert resp.status_code == 200, resp.text
    config = resp.json()["config"]
    assert not config.get("external_index_provider")
    assert config["configured_external_index_provider"]["type"] == "pinecone"


async def test_find_on_pinecone_kb(
    nucliadb_reader: AsyncClient,
    pinecone_knowledgebox: str,
    pinecone_data_plane,
):
    kbid = pinecone_knowledgebox

    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/find",
        json={"query": "own text"},
    )
    assert resp.status_code == 200, resp.text


async def test_ingestion_on_pinecone_kb(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    pinecone_knowledgebox: str,
    data_plane,
    mock_pinecone_client,
):
    kbid = pinecone_knowledgebox

    # Create a resource first
    slug = "my-resource"
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "slug": slug,
            "title": "Title Resource",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    bm = BrokerMessage(kbid=kbid, uuid=rid, slug=slug, type=BrokerMessage.AUTOCOMMIT)
    bm.basic.icon = "text/plain"
    bm.basic.title = "Title Resource"
    bm.basic.summary = "Summary of document"
    bm.basic.thumbnail = "doc"
    bm.basic.metadata.useful = True
    bm.basic.metadata.language = "es"
    bm.basic.created.FromDatetime(datetime.now())
    bm.basic.modified.FromDatetime(datetime.now())
    bm.origin.source = rpb.Origin.Source.WEB

    bm.texts["text"].body = "My own text Ramon. This is great to be here. \n Where is my beer?"
    bm.texts["text"].format = rpb.FieldText.Format.PLAIN

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "My own text Ramon. This is great to be here. \n Where is my beer?"
    etw.field.field = "text"
    etw.field.field_type = rpb.FieldType.TEXT
    bm.extracted_text.append(etw)

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

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "text"
    fcm.field.field_type = rpb.FieldType.TEXT
    p1 = rpb.Paragraph(
        start=0,
        end=45,
    )
    p1.start_seconds.append(0)
    p1.end_seconds.append(10)
    p2 = rpb.Paragraph(
        start=47,
        end=64,
    )
    p2.start_seconds.append(10)
    p2.end_seconds.append(20)
    p2.start_seconds.append(20)
    p2.end_seconds.append(30)

    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.paragraphs.append(p2)
    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())
    fcm.metadata.metadata.ner["Ramon"] = "PERSON"

    c1 = rpb.Classification()
    c1.label = "label1"
    c1.labelset = "labelset1"
    fcm.metadata.metadata.classifications.append(c1)
    bm.field_metadata.append(fcm)

    ev = rpb.ExtractedVectorsWrapper()
    ev.field.field = "text"
    ev.field.field_type = rpb.FieldType.TEXT

    v1 = rpb.Vector()
    v1.start = 0
    v1.end = 19
    v1.start_paragraph = 0
    v1.end_paragraph = 45
    v1.vector.extend([1.0, 2.0, 3.0])
    ev.vectors.vectors.vectors.append(v1)

    v2 = rpb.Vector()
    v2.start = 20
    v2.end = 45
    v2.start_paragraph = 0
    v2.end_paragraph = 45
    v2.vector.extend([4.0, 5.0, 6.0])
    ev.vectors.vectors.vectors.append(v2)

    v3 = rpb.Vector()
    v3.start = 48
    v3.end = 65
    v3.start_paragraph = 47
    v3.end_paragraph = 64
    v3.vector.extend([7.0, 8.0, 9.0])
    ev.vectors.vectors.vectors.append(v3)

    bm.field_vectors.append(ev)
    bm.source = BrokerMessage.MessageSource.PROCESSOR

    response = await nucliadb_grpc.ProcessMessage([bm], timeout=None)  # type: ignore
    assert response.status == OpStatusWriter.Status.OK

    assert data_plane.delete_by_id_prefix.await_count == 1
    assert data_plane.upsert_in_batches.await_count == 1
    upsert_call = data_plane.upsert_in_batches.call_args_list[0][1]
    assert len(upsert_call["vectors"]) == 3
