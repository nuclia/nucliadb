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

from nucliadb.writer.back_pressure import (
    TryAfterCache,
    estimate_try_after_from_rate,
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


async def test_check_processing_behind():
    # Check that it raises the http 429 exception
    # Check that it saves the try after in the cache
    pass


async def test_check_processing_behind_does_not_run_if_configured_max_is_zero():
    pass


async def test_check_processing_behind_does_not_run_on_cache_hit():
    pass


async def test_check_indexing_behind():
    # Check that it raises the http 429 exception
    # Check that it saves the try after in the cache
    pass


async def test_check_indexing_behind_does_not_run_if_configured_max_is_zero():
    pass


async def test_check_indexing_behind_does_not_run_on_cache_hit():
    pass
