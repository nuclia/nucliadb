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

from unittest.mock import ANY, MagicMock, patch

import pytest

from nucliadb.writer.tus.gcs import GCloudBlobStore


@pytest.mark.asyncio
@patch("nucliadb.writer.tus.gcs.aiohttp")
@patch("nucliadb.writer.tus.gcs.ServiceAccountCredentials")
async def test_tus_gcs(mock_sa, mock_aiohttp):
    mock_session = MagicMock()
    mock_aiohttp.ClientSession.return_value = mock_session
    mock_aenter = MagicMock()
    mock_aenter.__aenter__.return_value = MagicMock(status=200)
    mock_session.get.return_value = mock_aenter

    gblobstore = GCloudBlobStore()
    await gblobstore.initialize(
        bucket="test-bucket",
        location="test-location",
        project="test-project",
        bucket_labels="test-labels",
        object_base_url="test-url",
        json_credentials="dGVzdC1jcmVk",
    )

    mock_sa.from_json_keyfile_name.assert_called_once_with(
        ANY, ["https://www.googleapis.com/auth/devstorage.read_write"]
    )

    assert await gblobstore.check_exists("test-bucket")


@pytest.mark.asyncio
@patch("nucliadb.writer.tus.gcs.google")
async def test_tus_gcs_empty_json_credentials(mock_google):
    mock_google.auth.default.return_value = ("credential", "project")

    gblobstore = GCloudBlobStore()
    await gblobstore.initialize(
        bucket="test-bucket",
        location="test-location",
        project="test-project",
        bucket_labels="test-labels",
        object_base_url="test-url",
        json_credentials=None,
    )

    mock_google.auth.default.assert_called_once()
