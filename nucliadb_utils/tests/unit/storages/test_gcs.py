# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest.mock import AsyncMock, Mock, call, patch

import aiohttp
import pytest

from nucliadb_utils.storages.gcs import (
    GCSStorage,
    GCSStorageField,
    GoogleCloudException,
    ReadingResponseContentException,
)


@pytest.fixture(scope="function")
def storage_field():
    yield GCSStorageField(None, "bucket", "fullkey", None)  # type: ignore[ty:invalid-argument-type]  # First param should be a storage, ignoring it for unit testing


@pytest.fixture(scope="function")
def asyncio_sleep():
    with patch("nucliadb_utils.storages.gcs.asyncio.sleep") as asyncio_sleep:
        yield asyncio_sleep


class MockInnerIterData:
    def __init__(self, error=None, to_yield=None):
        self.error = error
        self.to_yield = to_yield
        self.await_count = 0

    async def __call__(self, **kwargs):
        self.await_count += 1
        if self.error:
            raise self.error
        for item in self.to_yield or []:
            yield item


@pytest.mark.parametrize("error", [GoogleCloudException, aiohttp.ServerDisconnectedError])
async def test_iter_data_error_retries(storage_field, error, asyncio_sleep):
    storage_field._inner_iter_data = MockInnerIterData(error=error)

    with pytest.raises(error):
        async for _ in storage_field.iter_data():
            pass

    storage_field._inner_iter_data.await_count == 4
    assert asyncio_sleep.call_args_list == [call(1), call(2), call(4)]


async def test_iter_data_reading_content_error_is_not_retried(storage_field):
    storage_field._inner_iter_data = MockInnerIterData(error=ReadingResponseContentException)

    with pytest.raises(ReadingResponseContentException):
        async for _ in storage_field.iter_data():
            pass

    storage_field._inner_iter_data.await_count == 1


async def test_delete_kb_errors():
    kbid = "my-kbid"
    storage = GCSStorage(bucket="bucket")

    # Let's mock session.delete to return a mock response
    response_mock = AsyncMock()

    delete_context = Mock()
    delete_context.__aenter__ = AsyncMock(return_value=response_mock)
    delete_context.__aexit__ = AsyncMock()

    storage._session = Mock()
    storage._session.delete = Mock(return_value=delete_context)

    # Test GCS responses and how we behave

    response_mock.status = 204
    deleted, conflict = await storage.delete_kb(kbid)
    assert deleted
    assert not conflict

    response_mock.status = 404
    deleted, conflict = await storage.delete_kb(kbid)
    assert not deleted
    assert not conflict

    response_mock.status = 409
    deleted, conflict = await storage.delete_kb(kbid)
    assert not deleted
    assert conflict

    response_mock.status = 500
    deleted, conflict = await storage.delete_kb(kbid)
    assert not deleted
    assert not conflict
