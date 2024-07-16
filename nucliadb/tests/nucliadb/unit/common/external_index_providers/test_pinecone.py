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
from unittest.mock import AsyncMock, MagicMock

import pytest

from nucliadb.common.external_index_providers.pinecone import PineconeIndexManager
from nucliadb_protos.noderesources_pb2 import IndexParagraph, IndexParagraphs
from nucliadb_protos.noderesources_pb2 import Resource as PBResourceBrain
from nucliadb_utils.aiopynecone.models import Vector


@pytest.fixture()
def data_plane():
    mock = MagicMock()
    mock.delete_by_id_prefix = AsyncMock()
    mock.upsert_in_batches = AsyncMock()
    return mock


@pytest.fixture()
def external_index_manager(data_plane):
    mock = MagicMock()
    mock.data_plane.return_value = data_plane
    with unittest.mock.patch(
        "nucliadb.common.external_index_providers.pinecone.get_pinecone", return_value=mock
    ):
        return PineconeIndexManager(
            kbid="kbid",
            api_key="api_key",
            index_host="index_host",
            upsert_parallelism=3,
            delete_parallelism=2,
            upsert_timeout=10,
            delete_timeout=10,
        )


async def test_delete_resource(external_index_manager: PineconeIndexManager, data_plane):
    await external_index_manager._delete_resource("resource_uuid")
    data_plane.delete_by_id_prefix.assert_awaited_once()
    resource_uuid = data_plane.delete_by_id_prefix.call_args[1]["id_prefix"]
    assert resource_uuid == "resource_uuid"


async def test_index_resource(external_index_manager: PineconeIndexManager, data_plane):
    index_data = PBResourceBrain()
    index_data.labels.extend(["/e/PERSON/John Doe", "/e/ORG/ACME", "/n/s/PROCESSED", "/t/private"])
    index_data.security.access_groups.extend(["ag1", "ag2"])
    index_paragraphs = IndexParagraphs()
    index_paragraph = IndexParagraph()
    index_paragraph.sentences["sid"].vector.extend([1, 2, 3])
    index_paragraphs.paragraphs["pid"].CopyFrom(index_paragraph)
    index_data.paragraphs["pid"].CopyFrom(index_paragraphs)

    await external_index_manager._index_resource("resource_uuid", index_data)
    data_plane.upsert_in_batches.assert_awaited_once()
    vectors = data_plane.upsert_in_batches.call_args[1]["vectors"]
    assert len(vectors) == 1
    vector = vectors[0]
    assert isinstance(vector, Vector)
    assert vector.id == "sid"
    assert vector.values == [1, 2, 3]
    metadata = vector.metadata
    assert "labels" in metadata
    labels = vector.metadata["labels"]
    # Check that ignored labels are not present
    assert "/e/PERSON/John Doe" not in labels
    assert "/e/ORG/ACME" not in labels
    assert "/n/s/PROCESSED" not in labels
    assert labels == ["/t/private"]
    assert "access_groups" in metadata
    access_groups = vector.metadata["access_groups"]
    assert sorted(access_groups) == ["ag1", "ag2"]
