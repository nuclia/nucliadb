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

import pytest

from nucliadb.common.external_index_providers.manager import get_external_index_manager
from nucliadb.common.external_index_providers.pinecone import PineconeIndexManager
from nucliadb_protos.knowledgebox_pb2 import (
    ExternalIndexProviderType,
    StoredExternalIndexProviderMetadata,
    StoredPineconeConfig,
)

MODULE = "nucliadb.common.external_index_providers.manager"


@pytest.fixture()
def endecryptor():
    with unittest.mock.patch(f"{MODULE}.get_endecryptor") as mock:
        mock.return_value.decrypt.return_value = "api_key"
        yield mock


async def test_get_external_index_manager_pinecone(endecryptor):
    stored_metadata = StoredExternalIndexProviderMetadata(
        type=ExternalIndexProviderType.PINECONE,
        pinecone_config=StoredPineconeConfig(
            encrypted_api_key="encrypted_api_key",
        ),
    )
    stored_metadata.pinecone_config.indexes["default--kbid"].index_host = "index_host"
    stored_metadata.pinecone_config.indexes["default--kbid"].vector_dimension = 10
    with unittest.mock.patch(
        f"{MODULE}.get_external_index_metadata",
        return_value=stored_metadata,
    ):
        mgr = await get_external_index_manager("kbid")
        assert isinstance(mgr, PineconeIndexManager)
        assert mgr.api_key == "api_key"
        assert mgr.index_hosts == {"default--kbid": "index_host"}


async def test_get_external_index_manager_none():
    with unittest.mock.patch(
        f"{MODULE}.get_external_index_metadata",
        return_value=None,
    ):
        assert await get_external_index_manager("kbid") is None
