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
import tempfile

import pytest

from nucliadb.writer.tus.gcs import GCloudBlobStore
from nucliadb.writer.tus.local import LocalBlobStore
from nucliadb.writer.tus.s3 import S3BlobStore


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


@pytest.fixture(scope="function")
async def local_storage_tus():
    folder = tempfile.TemporaryDirectory()
    storage = LocalBlobStore(local_testing_files=folder.name)
    await storage.initialize()
    yield storage
    await storage.finalize()
    folder.cleanup()
