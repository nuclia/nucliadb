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

import unittest
from unittest.mock import call

import aiohttp
import pytest

from nucliadb_utils.storages.gcs import (
    GCSStorageField,
    GoogleCloudException,
    ReadingResponseContentException,
)


@pytest.fixture(scope="function")
def storage_field():
    yield GCSStorageField("bucket", "fullkey", "field")


@pytest.fixture(scope="function")
def asyncio_sleep():
    with unittest.mock.patch(
        "nucliadb_utils.storages.gcs.asyncio.sleep"
    ) as asyncio_sleep:
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
        for item in self.to_yield:
            yield item


@pytest.mark.parametrize(
    "error", [GoogleCloudException, aiohttp.client_exceptions.ServerDisconnectedError]
)
async def test_iter_data_error_retries(storage_field, error, asyncio_sleep):
    storage_field._inner_iter_data = MockInnerIterData(error=error)

    with pytest.raises(error):
        async for _ in storage_field.iter_data():
            pass

    storage_field._inner_iter_data.await_count == 4
    assert asyncio_sleep.call_args_list == [call(1), call(2), call(4)]


async def test_iter_data_reading_content_error_is_not_retried(storage_field):
    storage_field._inner_iter_data = MockInnerIterData(
        error=ReadingResponseContentException
    )

    with pytest.raises(ReadingResponseContentException):
        async for _ in storage_field.iter_data():
            pass

    storage_field._inner_iter_data.await_count == 1
