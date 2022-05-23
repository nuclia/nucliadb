import pytest

from nucliadb_writer.tus.gcs import GCloudBlobStore
from nucliadb_writer.tus.s3 import S3BlobStore


@pytest.fixture(scope="function")
async def s3_storage_tus(s3):
    storage = S3BlobStore()
    await storage.initialize(
        client_id="",
        client_secret="",
        max_pool_connections=2,
        endpoint_url=s3,
        verify_ssl=False,
        ssl=False,
        region_name=None,
        bucket="test_{kbid}",
    )
    yield storage
    await storage.finalize()


@pytest.fixture(scope="function")
async def gcs_storage_tus(gcs):
    storage = GCloudBlobStore()
    await storage.initialize(
        json_credentials=None,
        bucket="test_{kbid}",
        location="location",
        project="project",
        bucket_labels={},
        object_base_url=gcs,
    )
    yield storage
    await storage.finalize()
