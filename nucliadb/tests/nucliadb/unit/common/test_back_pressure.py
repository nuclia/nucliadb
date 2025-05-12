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
import asyncio
import time
from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from nucliadb.common.back_pressure.cache import (
    BackPressureCache,
    BackPressureData,
    cached_back_pressure,
)
from nucliadb.common.back_pressure.cache import _cache as back_pressure_cache
from nucliadb.common.back_pressure.materializer import (
    BackPressureMaterializer,
    get_materializer,
    maybe_back_pressure,
    start_materializer,
)
from nucliadb.common.back_pressure.utils import (
    BackPressureException,
    estimate_try_after,
)

MODULE = "nucliadb.common.back_pressure"


@pytest.fixture(scope="function", autouse=True)
def is_back_pressure_enabled():
    with mock.patch(f"{MODULE}.materializer.is_back_pressure_enabled", return_value=True) as mock_:
        yield mock_


@pytest.mark.parametrize(
    "rate,pending,max_wait,delta",
    [
        (2, 10, 10, 5),
        (10, 10, 10, 1),
        (1, 10, 3, 3),
    ],
)
def test_estimate_try_after(rate, pending, max_wait, delta):
    now = datetime.now(timezone.utc)
    try_after = estimate_try_after(rate, pending, max_wait)
    assert int(try_after.timestamp()) == int(now.timestamp() + delta)


def test_back_pressure_cache():
    cache = BackPressureCache()

    key = "key"
    assert cache.get(key) is None

    # Set a value and get it
    now = datetime.now(timezone.utc)
    try_after = now + timedelta(seconds=0.5)
    data = BackPressureData(try_after=try_after, type="indexing")

    cache.set(key, data)

    assert cache.get(key) == data

    # Check that after try after has passed, it returns None
    time.sleep(0.6)
    assert cache.get(key) is None


async def test_maybe_back_pressure_skip_conditions_onprem(is_back_pressure_enabled, onprem_nucliadb):
    # onprem should never run back pressure even if enabled
    with mock.patch(f"{MODULE}.materializer.back_pressure_checks") as back_pressure_checks_mock:
        is_back_pressure_enabled.return_value = True
        await maybe_back_pressure(mock.Mock(), "kbid")
        back_pressure_checks_mock.assert_not_called()


async def test_maybe_back_pressure_skip_conditions_hosted(is_back_pressure_enabled, hosted_nucliadb):
    # Back pressure should only run for hosted deployments, when enabled
    with mock.patch(f"{MODULE}.materializer.back_pressure_checks") as back_pressure_checks_mock:
        is_back_pressure_enabled.return_value = False
        await maybe_back_pressure(mock.Mock(), "kbid")
        back_pressure_checks_mock.assert_not_called()

        is_back_pressure_enabled.return_value = True
        await maybe_back_pressure(mock.Mock(), "kbid")
        back_pressure_checks_mock.assert_awaited_once()


@pytest.fixture(scope="function")
def materializer():
    materializer = mock.Mock()
    materializer.running = True
    materializer.get_processing_pending = mock.AsyncMock(return_value=10)
    materializer.get_indexing_pending = mock.Mock(return_value={"node1": 10, "node2": 2})
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
        max_wait_time=60,
    )
    with mock.patch(f"{MODULE}.materializer.settings", settings):
        yield settings


@pytest.fixture(scope="function")
def cache():
    back_pressure_cache._cache.clear()
    yield back_pressure_cache


async def test_check_processing_behind(settings, cache, nats_conn):
    settings.max_processing_pending = 5

    materializer = BackPressureMaterializer(nats_conn)
    materializer.get_processing_pending = mock.AsyncMock(return_value=1)

    # Check that it runs and does not raise an exception if the pending is low
    await materializer.check_processing("kbid")
    materializer.get_processing_pending.assert_awaited_once_with("kbid")

    # Check that it raises an exception if the pending is too high
    materializer.get_processing_pending.reset_mock()
    materializer.get_processing_pending.return_value = 10
    with pytest.raises(BackPressureException):
        await materializer.check_processing("kbid")
    materializer.get_processing_pending.assert_awaited_once_with("kbid")


async def test_check_processing_behind_does_not_run_if_configured_max_is_zero(settings, cache):
    settings.max_processing_pending = 0
    materializer = BackPressureMaterializer(mock.Mock())
    materializer.get_processing_pending = mock.AsyncMock(return_value=100)

    await materializer.check_processing("kbid")

    materializer.get_processing_pending.assert_not_called()


async def test_check_ingest_behind(settings, cache):
    materializer = BackPressureMaterializer(mock.Mock())
    settings.max_ingest_pending = 5
    materializer.ingest_pending = 2
    materializer.check_ingest()

    with pytest.raises(BackPressureException):
        materializer.ingest_pending = 10
        materializer.check_ingest()


async def test_check_ingest_behind_does_not_raise_if_configured_max_is_zero(settings):
    settings.max_ingest_pending = 0
    materializer = BackPressureMaterializer(mock.Mock())
    materializer.ingest_pending = 1000
    materializer.check_ingest()


def test_cached_back_pressure_context_manager(cache):
    func = mock.Mock()

    with cached_back_pressure("foo-bar"):
        func()

    func.assert_called_once()

    func.reset_mock()
    func.side_effect = Exception("Boom")

    with pytest.raises(Exception):
        with cached_back_pressure("foo-bar"):
            func()

    func.reset_mock()

    data = BackPressureData(
        try_after=datetime.now(timezone.utc) + timedelta(seconds=10), type="indexing"
    )
    func.side_effect = BackPressureException(data)

    with pytest.raises(BackPressureException) as exc:
        with cached_back_pressure("foo-bar"):
            func()
    assert exc.value.data == data


@pytest.fixture(scope="function")
def js():
    consumer_info = mock.Mock(num_pending=10)
    js = mock.Mock()
    js.consumer_info = mock.AsyncMock(return_value=consumer_info)
    yield js


@pytest.fixture(scope="function")
def nats_conn(js):
    ncm = mock.Mock()
    ncm.js = js
    yield ncm


@pytest.fixture(scope="function")
def processing_client():
    processing_client = mock.Mock()
    resp = mock.Mock(incomplete=10)
    processing_client.stats = mock.AsyncMock(return_value=resp)
    processing_client.close = mock.AsyncMock()
    yield processing_client


async def test_materializer(nats_conn, js, processing_client):
    materializer = BackPressureMaterializer(
        nats_conn,
        indexing_check_interval=0.5,
        ingest_check_interval=0.5,
    )
    materializer.processing_http_client = processing_client

    assert not materializer.running
    await materializer.start()

    # Make sure the tasks are running
    assert materializer.running
    assert len(materializer._tasks) == 2

    await asyncio.sleep(0.1)

    # one index nodes and ingest streams are queried
    assert len(js.consumer_info.call_args_list) == 2

    # Wait for the next check
    await asyncio.sleep(0.5)

    assert len(js.consumer_info.call_args_list) == 4

    # Make sure the values are materialized
    assert materializer.get_indexing_pending() == 10
    assert materializer.get_ingest_pending() == 10

    # Make sure processing pending are cached
    assert await materializer.get_processing_pending("kbid") == 10
    assert await materializer.get_processing_pending("kbid") == 10
    materializer.processing_http_client.stats.assert_awaited_once_with(kbid="kbid", timeout=0.5)

    await materializer.stop()

    # Make sure tasks are cancelled
    assert materializer._tasks == []

    # Make sure the processing client is closed
    materializer.processing_http_client.close.assert_awaited_once()


async def test_start_materializer():
    nats_mgr = mock.Mock()
    context = mock.Mock(nats_manager=nats_mgr)

    await start_materializer(context)
    mat = get_materializer()
    assert isinstance(mat, BackPressureMaterializer)
    assert mat.nats_manager == nats_mgr

    assert mat.running

    # Make sure the singleton is set
    from nucliadb.common.back_pressure.materializer import MATERIALIZER

    assert MATERIALIZER is mat

    await mat.stop()
