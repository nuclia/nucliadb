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
import unittest
from unittest import mock
from unittest.mock import call

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb.common.external_index_providers.pinecone import PineconeIndexManager
from nucliadb_protos.knowledgebox_pb2 import (
    CreateExternalIndexProviderMetadata,
    CreatePineconeConfig,
    ExternalIndexProviderType,
    KnowledgeBoxConfig,
    KnowledgeBoxID,
    KnowledgeBoxNew,
    KnowledgeBoxResponseStatus,
    NewKnowledgeBoxResponse,
)
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_protos.writer_pb2 import NewKnowledgeBoxV2Request
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.utilities import get_endecryptor


@pytest.fixture(autouse=True)
def mock_pinecone_client():
    session_mock = mock.MagicMock()
    pinecone_mock = mock.MagicMock()
    pinecone_mock.create_index = mock.AsyncMock(return_value="pinecone-host")
    pinecone_mock.delete_index = mock.AsyncMock(return_value=None)
    session_mock.control_plane.return_value = pinecone_mock
    with unittest.mock.patch("nucliadb.ingest.orm.knowledgebox.get_pinecone", return_value=session_mock):
        yield pinecone_mock


@pytest.fixture(autouse=True)
def hosted_nucliadb():
    with unittest.mock.patch("nucliadb.ingest.service.writer.is_onprem_nucliadb", return_value=False):
        yield


@pytest.mark.asyncio
async def test_kb_creation_old(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    mock_pinecone_client,
):
    """
    This tests the deprecated method for creating kbs on a hosted nucliadb that
    uses the nucliadb_grpc.NewKnowledgeBox method.
    """
    kbid = f"test_pinecone_kb_{random.randint(0, 1000000)}"
    slug = kbid
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
            ),
        ),
    )

    # Creating a knowledge box should create a Pinecone index
    response: NewKnowledgeBoxResponse = await nucliadb_grpc.NewKnowledgeBox(request, timeout=None)  # type: ignore
    assert response.status == KnowledgeBoxResponseStatus.OK

    expected_index_name = PineconeIndexManager.get_index_name(kbid, "default")
    mock_pinecone_client.create_index.assert_awaited_once_with(name=expected_index_name, dimension=128)

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
        assert (
            external_index_provider.pinecone_config.indexes[expected_index_name].index_host
            == "pinecone-host"
        )
        assert (
            external_index_provider.pinecone_config.indexes[expected_index_name].vector_dimension == 128
        )

    # Deleting a knowledge box should delete the Pinecone index
    response = await nucliadb_grpc.DeleteKnowledgeBox(KnowledgeBoxID(slug=slug, uuid=kbid), timeout=None)  # type: ignore
    assert response.status == KnowledgeBoxResponseStatus.OK

    mock_pinecone_client.delete_index.assert_awaited_once_with(name=expected_index_name)


@pytest.mark.asyncio
async def test_kb_creation_new(
    nucliadb_grpc: WriterStub,
    nucliadb_reader: AsyncClient,
    mock_pinecone_client,
):
    """
    This tests the new method for creating kbs on a hosted nucliadb that
    uses the nucliadb_grpc.NewKnowledgeBoxV2 method.
    """

    kbid = f"test_pinecone_kb_{random.randint(0, 1000000)}"
    slug = kbid
    request = NewKnowledgeBoxV2Request(
        kbid=kbid,
        slug=slug,
        title="Pinecone test",
        description="Description",
        external_index_provider=CreateExternalIndexProviderMetadata(
            type=ExternalIndexProviderType.PINECONE,
            pinecone_config=CreatePineconeConfig(
                api_key="my-pinecone-api-key",
            ),
        ),
    )
    vectorset_1_id = "multilingual-2024-01-01"
    request.vectorsets.append(
        NewKnowledgeBoxV2Request.VectorSet(
            vectorset_id=vectorset_1_id,
            similarity=VectorSimilarity.DOT,
            vector_dimension=128,
        )
    )
    vectorset_2_id = "english-2022-01-01"
    request.vectorsets.append(
        NewKnowledgeBoxV2Request.VectorSet(
            vectorset_id=vectorset_2_id,
            similarity=VectorSimilarity.DOT,
            vector_dimension=3,
        )
    )

    # Creating a knowledge with 2 vectorsets box should create two Pinecone indexes
    response: NewKnowledgeBoxResponse = await nucliadb_grpc.NewKnowledgeBoxV2(request, timeout=None)  # type: ignore
    assert response.status == KnowledgeBoxResponseStatus.OK

    # Should create an index for every vectorset
    expected_index_names = [
        PineconeIndexManager.get_index_name(kbid, vectorset_1_id),
        PineconeIndexManager.get_index_name(kbid, vectorset_2_id),
    ]
    mock_pinecone_client.create_index.assert_has_calls(
        [
            call(name=expected_index_names[0], dimension=128),
            call(name=expected_index_names[1], dimension=3),
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
        assert pinecone_config.indexes[expected_index_names[0]].index_host == "pinecone-host"
        assert pinecone_config.indexes[expected_index_names[0]].vector_dimension == 128
        assert pinecone_config.indexes[expected_index_names[1]].index_host == "pinecone-host"
        assert pinecone_config.indexes[expected_index_names[1]].vector_dimension == 3

    # Deleting a knowledge box should delete the Pinecone index
    response = await nucliadb_grpc.DeleteKnowledgeBox(KnowledgeBoxID(slug=slug, uuid=kbid), timeout=None)  # type: ignore
    assert response.status == KnowledgeBoxResponseStatus.OK

    # Test that deletes all external indexes
    mock_pinecone_client.delete_index.assert_has_calls(
        [call(name=expected_index_names[0]), call(name=expected_index_names[1])]
    )


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
    assert resp.status_code == 200
