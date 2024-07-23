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
import logging
import os
from pathlib import Path
from typing import AsyncIterator
from unittest.mock import patch

import psycopg
import pytest
from pytest import FixtureRequest
from pytest_docker_fixtures import images
from pytest_lazy_fixtures import lazy_fixture

from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.local import LocalDriver
from nucliadb.common.maindb.pg import PGDriver
from nucliadb.ingest.settings import DriverConfig, DriverSettings
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb.migrator.migrator import run_pg_schema_migrations
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    set_utility,
)

logger = logging.getLogger("nucliadb.fixtures:maindb")

# Minimum support PostgreSQL version
# Reason: We want the btree_gin extension to support uuid's
images.settings["postgresql"]["version"] = "11"
images.settings["postgresql"]["env"]["POSTGRES_PASSWORD"] = "postgres"


def maindb_settings_lazy_fixtures(default_drivers="local"):
    driver_types = os.environ.get("TESTING_MAINDB_DRIVERS", default_drivers)
    return [lazy_fixture.lf(f"{driver_type}_maindb_settings") for driver_type in driver_types.split(",")]


@pytest.fixture(scope="function", params=maindb_settings_lazy_fixtures())
def maindb_settings(request: FixtureRequest):
    """
    Allows dynamically loading the driver fixtures via env vars.

    TESTING_MAINDB_DRIVERS=redis,local pytest nucliadb/tests/

    Any test using the nucliadb fixture will be run twice, once with redis driver and once with local driver.
    """
    yield request.param


def maindb_driver_lazy_fixtures(default_drivers: str = "pg"):
    """
    Allows running tests using maindb_driver for each supported driver type via env vars.

    TESTING_MAINDB_DRIVERS=redis,local pytest nucliadb/tests/ingest

    Any test using the maindb_driver fixture will be run twice, once with redis_driver and once with local_driver.
    """
    driver_types = os.environ.get("TESTING_MAINDB_DRIVERS", default_drivers)
    return [lazy_fixture.lf(f"{driver_type}_maindb_driver") for driver_type in driver_types.split(",")]


@pytest.fixture(
    scope="function",
    params=maindb_driver_lazy_fixtures(),
)
async def maindb_driver(request: FixtureRequest) -> AsyncIterator[Driver]:
    driver: Driver = request.param
    set_utility(Utility.MAINDB_DRIVER, driver)

    yield driver

    clean_utility(Utility.MAINDB_DRIVER)

    # cleanup maindb
    if driver.initialized:
        try:
            async with driver.transaction() as txn:
                all_keys = [k async for k in txn.keys("", count=-1)]
                for key in all_keys:
                    await txn.delete(key)
                await txn.commit()
        except Exception:
            logger.exception("Could not cleanup maindb on test teardown")


@pytest.fixture(scope="function")
async def pg_maindb_settings(pg):
    url = f"postgresql://postgres:postgres@{pg[0]}:{pg[1]}/postgres"

    driver = PGDriver(url=url, connection_pool_min_size=2, connection_pool_max_size=2)
    await driver.initialize()
    await run_pg_schema_migrations(driver)

    return DriverSettings(
        driver=DriverConfig.PG,
        driver_pg_url=url,
    )


@pytest.fixture(scope="function")
async def pg_maindb_driver(pg_maindb_settings: DriverSettings):
    url = pg_maindb_settings.driver_pg_url
    assert url is not None
    with (
        patch.object(ingest_settings, "driver", DriverConfig.PG),
        patch.object(ingest_settings, "driver_pg_url", url),
    ):
        async with await psycopg.AsyncConnection.connect(url) as conn, conn.cursor() as cur:
            await cur.execute("DROP table IF EXISTS migrations")
            await cur.execute("DROP table IF EXISTS resources")
            await cur.execute("DROP table IF EXISTS catalog")

        driver = PGDriver(url=url)
        await driver.initialize()
        await run_pg_schema_migrations(driver)

        yield driver

        await driver.finalize()


@pytest.fixture(scope="function")
def local_maindb_settings(tmp_path: Path):
    return DriverSettings(
        driver=DriverConfig.LOCAL,
        driver_local_url=str((tmp_path / "main").absolute()),
    )


@pytest.fixture(scope="function")
async def local_maindb_driver(local_maindb_settings: DriverSettings) -> AsyncIterator[Driver]:
    path = local_maindb_settings.driver_local_url
    assert path is not None
    with (
        patch.object(ingest_settings, "driver", DriverConfig.LOCAL),
        patch.object(ingest_settings, "driver_local_url", path),
    ):
        driver: Driver = LocalDriver(url=path)
        await driver.initialize()

        yield driver

        await driver.finalize()
