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
import time
from datetime import datetime, timedelta
from unittest import mock

import pytest
from fastapi import HTTPException

from nucliadb.writer.back_pressure import (
    TryAfterCache,
    check_indexing_behind,
    check_ingest_behind,
    check_processing_behind,
    estimate_try_after_from_rate,
    maybe_back_pressure,
    try_after_cache,
)

MODULE = "nucliadb.writer.back_pressure"


@pytest.fixture(scope="function", autouse=True)
def is_back_pressure_enabled():
    with mock.patch(f"{MODULE}.is_back_pressure_enabled", return_value=True) as mock_:
        yield mock_


@pytest.fixture(scope="function", autouse=True)
def is_onprem_nucliadb():
    with mock.patch(f"{MODULE}.is_onprem_nucliadb", return_value=False) as mock_:
        yield mock_


@pytest.mark.parametrize(
    "rate,pending,delta",
    [
        (2, 10, 5),
        (10, 10, 1),
    ],
)
def test_estimate_try_after_from_rate(rate, pending, delta):
    now = datetime.utcnow()
    try_after = estimate_try_after_from_rate(rate, pending)
    assert int(try_after.timestamp()) == int(now.timestamp() + delta)


def test_try_after_cache():
    cache = TryAfterCache()

    key = "key"
    assert cache.get(key) is None

    # Set a value and get it
    now = datetime.utcnow()
    try_after = now + timedelta(seconds=0.5)
    cache.set(key, try_after)

    assert cache.get(key) == try_after

    time.sleep(0.6)
    assert cache.get(key) is None


async def test_maybe_back_pressure_skip_conditions(
    is_back_pressure_enabled, is_onprem_nucliadb
):
    with mock.patch(
        f"{MODULE}.check_processing_behind"
    ) as check_processing_behind_mock, mock.patch(
        f"{MODULE}.check_indexing_behind", return_value=False
    ) as check_indexing_behind_mock:
        # Check that if back pressure is not enabled, it should not run
        is_back_pressure_enabled.return_value = False
        is_onprem_nucliadb.return_value = False

        await maybe_back_pressure(mock.Mock(), "kbid")

        check_processing_behind_mock.assert_not_called()
        check_indexing_behind_mock.assert_not_called()

        # Even if enabled, it should not run if not on-prem
        is_back_pressure_enabled.return_value = True
        is_onprem_nucliadb.return_value = True

        await maybe_back_pressure(mock.Mock(), "kbid")

        check_processing_behind_mock.assert_not_called()
        check_indexing_behind_mock.assert_not_called()

        # It should run only if not on-prem and enabled
        is_back_pressure_enabled.return_value = True
        is_onprem_nucliadb.return_value = False

        await maybe_back_pressure(mock.Mock(), "kbid")

        check_processing_behind_mock.assert_awaited_once()
        check_indexing_behind_mock.assert_awaited_once()


@pytest.fixture(scope="function")
def get_pending_to_process():
    with mock.patch(f"{MODULE}.get_pending_to_process", return_value=10) as mock_:
        yield mock_


@pytest.fixture(scope="function")
def get_pending_to_index():
    with mock.patch(
        f"{MODULE}.get_pending_to_index", return_value={"node1": 10, "node2": 2}
    ) as mock_:
        yield mock_


@pytest.fixture(scope="function")
def get_pending_to_ingest():
    with mock.patch(f"{MODULE}.get_pending_to_ingest", return_value=10) as mock_:
        yield mock_


@pytest.fixture(scope="function")
def settings():
    settings = mock.Mock(
        max_ingest_pending=10,
        max_processing_pending=10,
        max_indexing_pending=10,
        processing_rate=2,
        indexing_rate=2,
        ingest_rate=2,
    )
    with mock.patch(f"{MODULE}.settings", settings):
        yield settings


@pytest.fixture(scope="function")
def cache():
    try_after_cache._cache.clear()
    yield try_after_cache


async def test_check_processing_behind(get_pending_to_process, settings, cache):
    # Check that it raises the http 429 exception
    settings.max_processing_pending = 5
    get_pending_to_process.return_value = 10

    with pytest.raises(HTTPException) as exc:
        await check_processing_behind()
    assert exc.value.status_code == 429
    assert exc.value.detail["back_pressure_type"] == "processing"

    get_pending_to_process.assert_awaited()

    # Check that it saves the try after in the cache
    cache.get("processing") is not None


async def test_check_processing_behind_does_not_run_if_configured_max_is_zero(
    get_pending_to_process, settings, cache
):
    # Check that it raises the http 429 exception
    settings.max_processing_pending = 0
    get_pending_to_process.return_value = 100

    await check_processing_behind()

    get_pending_to_process.assert_not_called()


async def test_check_processing_behind_does_not_run_on_cache_hit(
    get_pending_to_process, settings, cache
):
    settings.max_processing_pending = 1
    get_pending_to_process.return_value = 10

    cache.set("processing", datetime.utcnow() + timedelta(seconds=2))

    with pytest.raises(HTTPException) as exc:
        await check_processing_behind()
    assert exc.value.status_code == 429
    assert exc.value.detail["back_pressure_type"] == "processing"

    get_pending_to_process.assert_not_called()


async def test_check_indexing_behind(get_pending_to_index, settings, cache):
    # Check that it raises the http 429 exception
    settings.max_indexing_pending = 5
    get_pending_to_index.return_value = {"node1": 10, "node2": 9}

    with pytest.raises(HTTPException) as exc:
        await check_indexing_behind(mock.Mock(), "kbid", resource_uuid="rid")

    assert exc.value.status_code == 429
    assert exc.value.detail["back_pressure_type"] == "indexing"

    get_pending_to_index.assert_awaited()

    # Check that it saves the try after in the cache
    cache.get("kbid::rid") is not None


async def test_check_indexing_behind_does_not_run_if_configured_max_is_zero(
    get_pending_to_index, settings
):
    # Check that it raises the http 429 exception
    settings.max_indexing_pending = 0
    get_pending_to_index.return_value = 100

    await check_indexing_behind(mock.Mock(), "kbid", resource_uuid="rid")

    get_pending_to_index.assert_not_called()


async def test_check_indexing_behind_does_not_run_on_cache_hit(
    get_pending_to_index, settings, cache
):
    settings.max_indexing_pending = 1
    get_pending_to_index.return_value = 10

    cache.set("kbid::rid", datetime.utcnow() + timedelta(seconds=2))

    with pytest.raises(HTTPException) as exc:
        await check_indexing_behind(mock.Mock(), "kbid", resource_uuid="rid")

    assert exc.value.status_code == 429
    assert exc.value.detail["back_pressure_type"] == "indexing"

    get_pending_to_index.assert_not_called()


async def test_check_ingest_behind(get_pending_to_ingest, settings, cache):
    # Check that it raises the http 429 exception
    settings.max_ingest_pending = 5
    get_pending_to_ingest.return_value = 10

    with pytest.raises(HTTPException) as exc:
        await check_ingest_behind(mock.Mock())

    assert exc.value.status_code == 429
    assert exc.value.detail["back_pressure_type"] == "ingest"

    get_pending_to_ingest.assert_awaited()

    # Check that it saves the try after in the cache
    cache.get("ingest") is not None


async def test_check_ingest_behind_does_not_run_if_configured_max_is_zero(
    get_pending_to_ingest, settings
):
    # Check that it raises the http 429 exception
    settings.max_ingest_pending = 0
    get_pending_to_ingest.return_value = 100

    await check_ingest_behind(mock.Mock())

    get_pending_to_ingest.assert_not_called()


async def test_check_ingest_behind_does_not_run_on_cache_hit(
    get_pending_to_ingest, settings, cache
):
    settings.max_ingest_pending = 1
    get_pending_to_ingest.return_value = 10

    cache.set("ingest", datetime.utcnow() + timedelta(seconds=2))

    with pytest.raises(HTTPException) as exc:
        await check_ingest_behind(mock.Mock())

    assert exc.value.status_code == 429
    assert exc.value.detail["back_pressure_type"] == "ingest"

    get_pending_to_ingest.assert_not_called()
