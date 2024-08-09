from unittest.mock import MagicMock, patch

import pytest

from nucliadb.writer.tus.gcs import GCloudBlobStore


@pytest.mark.asyncio
@patch("nucliadb.writer.tus.gcs.aiohttp")
@patch("nucliadb.writer.tus.gcs.service_account")
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
        json_credentials="test-cred",
    )

    mock_sa.Credentials.from_service_account_info.assert_called_once_with(
        "test-cred", scopes=["https://www.googleapis.com/auth/devstorage.read_write"]
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
