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

from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.writer import tus
from nucliadb.writer.tus.azure import AzureBlobStore
from nucliadb_utils.settings import FileBackendConfig

ACCOUNT_URL = "https://main.blob.core.windows.net"
KB_ACCOUNT_URL = "https://kbs.blob.core.windows.net"


@pytest.fixture()
def azure_initialize():
    with (
        patch.object(tus.storage_settings, "file_backend", FileBackendConfig.AZURE),
        patch.object(tus.storage_settings, "azure_account_url", ACCOUNT_URL),
        patch.object(tus.storage_settings, "azure_connection_string", None),
        patch.object(AzureBlobStore, "initialize", AsyncMock()) as initialize,
    ):
        yield initialize
    tus.DRIVER = None


async def test_azure_driver_uses_kb_account_url(azure_initialize):
    with patch.object(tus.storage_settings, "azure_kb_account_url", KB_ACCOUNT_URL):
        await tus.initialize()

    azure_initialize.assert_awaited_once_with(KB_ACCOUNT_URL, connection_string=None)


async def test_azure_driver_defaults_to_account_url(azure_initialize):
    with patch.object(tus.storage_settings, "azure_kb_account_url", None):
        await tus.initialize()

    azure_initialize.assert_awaited_once_with(ACCOUNT_URL, connection_string=None)
