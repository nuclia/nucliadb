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

import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nucliadb_utils import featureflagging, utilities
from nucliadb_utils.exceptions import ConfigurationError


@pytest.fixture(autouse=True)
def reset_main():
    utilities.MAIN.clear()


def test_clean_utility():
    utilities.set_utility(utilities.Utility.PUBSUB, "test")
    assert utilities.get_utility(utilities.Utility.PUBSUB) == "test"
    utilities.clean_utility(utilities.Utility.PUBSUB)
    assert utilities.get_utility(utilities.Utility.PUBSUB) is None


@pytest.mark.asyncio
async def test_get_storage_s3():
    s3 = AsyncMock()
    with patch.object(utilities.storage_settings, "file_backend", "s3"), patch(
        "nucliadb_utils.storages.s3.S3Storage", return_value=s3
    ):
        assert await utilities.get_storage() == s3


@pytest.mark.asyncio
async def test_get_storage_gcs():
    gcs = AsyncMock()
    with patch.object(utilities.storage_settings, "file_backend", "gcs"), patch(
        "nucliadb_utils.storages.gcs.GCSStorage", return_value=gcs
    ):
        assert await utilities.get_storage() == gcs


@pytest.mark.asyncio
async def test_get_storage_pg():
    pg = AsyncMock()
    with patch.object(utilities.storage_settings, "file_backend", "pg"), patch(
        "nucliadb_utils.storages.pg.PostgresStorage", return_value=pg
    ):
        assert await utilities.get_storage() == pg


@pytest.mark.asyncio
async def test_get_storage_local():
    local = AsyncMock()
    with patch.object(
        utilities.storage_settings, "file_backend", "local"
    ), patch.object(utilities.storage_settings, "local_files", "/files"), patch(
        "nucliadb_utils.storages.local.LocalStorage", return_value=local
    ):
        assert await utilities.get_storage() == local


@pytest.mark.asyncio
async def test_get_storage_missing():
    with patch.object(utilities.storage_settings, "file_backend", "missing"):
        with pytest.raises(ConfigurationError):
            await utilities.get_storage()


@pytest.mark.asyncio
async def test_get_local_storage():
    assert utilities.get_local_storage() is not None


@pytest.mark.asyncio
async def test_get_nuclia_storage():
    assert await utilities.get_nuclia_storage() is not None


@pytest.mark.asyncio
async def test_get_pubsub():
    with patch("nucliadb_utils.utilities.NatsPubsub", return_value=AsyncMock()):
        assert await utilities.get_pubsub() is not None


@pytest.mark.asyncio
async def test_finalize_utilities():
    util = AsyncMock()
    utilities.MAIN["test"] = util

    await utilities.finalize_utilities()

    util.finalize.assert_called_once()
    assert len(utilities.MAIN) == 0


@pytest.mark.asyncio
async def test_start_audit_utility():
    with patch("nucliadb_utils.utilities.NatsPubsub", return_value=AsyncMock()), patch(
        "nucliadb_utils.utilities.StreamAuditStorage", return_value=AsyncMock()
    ):
        await utilities.start_audit_utility("service")

        assert "audit" in utilities.MAIN


@pytest.mark.asyncio
async def test_stop_audit_utility():
    with patch("nucliadb_utils.utilities.NatsPubsub", return_value=AsyncMock()), patch(
        "nucliadb_utils.utilities.StreamAuditStorage", return_value=AsyncMock()
    ):
        await utilities.start_audit_utility("service")
        await utilities.stop_audit_utility()

        assert "audit" not in utilities.MAIN


def test_get_feature_flags():
    ff = utilities.get_feature_flags()
    assert ff is not None
    assert isinstance(ff, featureflagging.FlagService)


def test_has_feature():
    ff = MagicMock()
    headers = {
        utilities.X_USER_HEADER: "user",
        utilities.X_ACCOUNT_HEADER: "account",
        utilities.X_ACCOUNT_TYPE_HEADER: "account-type",
    }
    with patch("nucliadb_utils.utilities.get_feature_flags", return_value=ff):
        ff.enabled.return_value = True
        assert utilities.has_feature("test", default=False, headers=headers)

        ff.enabled.assert_called_once_with(
            "test",
            default=False,
            context={
                "user_id_md5": hashlib.md5(b"user").hexdigest(),
                "account_id_md5": hashlib.md5(b"account").hexdigest(),
                "account_type": "account-type",
            },
        )
