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
    BackPressureCache,
    BackPressureData,
    BackPressureException,
)
from nucliadb.writer.back_pressure import _cache as back_pressure_cache
from nucliadb.writer.back_pressure import (
    cached_back_pressure,
    check_indexing_behind,
    check_ingest_behind,
    check_processing_behind,
    estimate_try_after,
    maybe_back_pressure,
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
def test_estimate_try_after(rate, pending, delta):
    now = datetime.utcnow()
    try_after = estimate_try_after(rate, pending)
    assert int(try_after.timestamp()) == int(now.timestamp() + delta)


def test_back_pressure_cache():
    cache = BackPressureCache()

    key = "key"
    assert cache.get(key) is None

    # Set a value and get it
    now = datetime.utcnow()
    try_after = now + timedelta(seconds=0.5)
    data = BackPressureData(try_after=try_after, type="indexing")

    cache.set(key, data)

    assert cache.get(key) == data

    # Check that after try after has passed, it returns None
    time.sleep(0.6)
    assert cache.get(key) is None


async def test_maybe_back_pressure_skip_conditions(
    is_back_pressure_enabled, is_onprem_nucliadb
):
    with mock.patch(f"{MODULE}.back_pressure_checks") as back_pressure_checks_mock:
        # Check that if back pressure is not enabled, it should not run
        is_back_pressure_enabled.return_value = False
        is_onprem_nucliadb.return_value = False

        await maybe_back_pressure(mock.Mock(), "kbid")

        back_pressure_checks_mock.assert_not_called()

        # Even if enabled, it should not run if not on-prem
        is_back_pressure_enabled.return_value = True
        is_onprem_nucliadb.return_value = True

        await maybe_back_pressure(mock.Mock(), "kbid")

        back_pressure_checks_mock.assert_not_called()

        # It should run only if not on-prem and enabled
        is_back_pressure_enabled.return_value = True
        is_onprem_nucliadb.return_value = False

        await maybe_back_pressure(mock.Mock(), "kbid")

        back_pressure_checks_mock.assert_awaited_once()


@pytest.fixture(scope="function")
def materializer():
    materializer = mock.Mock()
    materializer.running = True
    materializer.get_processing_pending = mock.AsyncMock(return_value=10)
    materializer.get_indexing_pending = mock.Mock(
        return_value={"node1": 10, "node2": 2}
    )
    materializer.get_ingest_pending = mock.Mock(return_value=10)
    yield materializer


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
    back_pressure_cache._cache.clear()
    yield back_pressure_cache


async def test_check_processing_behind(materializer, settings, cache):
    settings.max_processing_pending = 5

    # Check that it runs and does not raise an exception if the pending is low
    materializer.get_processing_pending.return_value = 1
    await check_processing_behind(materializer, "kbid")
    materializer.get_processing_pending.assert_awaited_once_with("kbid")

    # Check that it raises an exception if the pending is too high
    materializer.get_processing_pending.reset_mock()
    materializer.get_processing_pending.return_value = 10
    with pytest.raises(BackPressureException):
        await check_processing_behind(materializer, "kbid")
    materializer.get_processing_pending.assert_awaited_once_with("kbid")


async def test_check_processing_behind_does_not_run_if_configured_max_is_zero(
    materializer, settings, cache
):
    settings.max_processing_pending = 0
    materializer.get_processing_pending.return_value = 100

    await check_processing_behind(materializer, "kbid")

    materializer.get_processing_pending.assert_not_called()


@pytest.fixture(scope="function")
def get_nodes_for_resource_shard():
    with mock.patch(
        f"{MODULE}.get_nodes_for_resource_shard", return_value=["node1", "node2"]
    ) as mock_:
        yield mock_


async def test_check_indexing_behind(get_nodes_for_resource_shard, settings, cache):
    settings.max_indexing_pending = 5
    context = mock.Mock()

    # Check that it runs and does not raise an exception if the pending is low
    await check_indexing_behind(context, "kbid", "rid", {"node1": 0, "node2": 2})
    get_nodes_for_resource_shard.assert_awaited_once_with(context, "kbid", "rid")

    # Check that it raises an exception if the pending is too high
    get_nodes_for_resource_shard.reset_mock()
    with pytest.raises(BackPressureException):
        await check_indexing_behind(context, "kbid", "rid", {"node1": 10, "node2": 2})
    get_nodes_for_resource_shard.assert_awaited_once_with(context, "kbid", "rid")


async def test_check_indexing_behind_does_not_run_if_configured_max_is_zero(
    get_nodes_for_resource_shard, settings
):
    settings.max_indexing_pending = 0

    await check_indexing_behind(mock.Mock(), "kbid", "rid", {"node1": 100})

    get_nodes_for_resource_shard.assert_not_called()


def test_check_ingest_behind(settings, cache):
    settings.max_ingest_pending = 5

    check_ingest_behind(2)

    with pytest.raises(BackPressureException):
        check_ingest_behind(10)


def test_check_ingest_behind_does_not_raise_if_configured_max_is_zero(settings):
    settings.max_ingest_pending = 0

    check_ingest_behind(1000)


def test_cached_back_pressure_context_manager(cache):
    func = mock.Mock()

    with cached_back_pressure("foo", "bar"):
        func()

    func.assert_called_once()

    func.reset_mock()
    func.side_effect = Exception("Boom")

    with pytest.raises(Exception):
        with cached_back_pressure("foo", "bar"):
            func()

    func.reset_mock()

    data = BackPressureData(
        try_after=datetime.now() + timedelta(seconds=10), type="indexing"
    )
    func.side_effect = BackPressureException(data)

    with pytest.raises(HTTPException) as exc:
        with cached_back_pressure("foo", "bar"):
            func()

    assert exc.value.status_code == 429
    assert exc.value.detail["message"].startswith(
        "Too many messages pending to ingest. Retry after"
    )
    assert exc.value.detail["try_after"]
    assert exc.value.detail["back_pressure_type"] == "indexing"
