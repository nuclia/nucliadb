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
from typing import AsyncIterator
from unittest.mock import patch

import psycopg
import pytest
from pytest_docker_fixtures import images

from nucliadb.common.maindb.driver import Driver
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


@pytest.fixture(scope="function")
def maindb_settings(pg_maindb_settings):
    yield pg_maindb_settings


@pytest.fixture(scope="function")
async def maindb_driver(pg_maindb_driver) -> AsyncIterator[Driver]:
    driver: Driver = pg_maindb_driver
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

    # We want to be sure schema migrations are always run. As some tests use
    # this fixture and create their own driver, we need to create one here and
    # run the migrations, so the maindb_settings fixture can still be generic
    # and pg migrations are run
    driver = PGDriver(url=url, connection_pool_min_size=2, connection_pool_max_size=2)
    await driver.initialize()
    await run_pg_schema_migrations(driver)
    await driver.finalize()

    yield DriverSettings(
        driver=DriverConfig.PG,
        driver_pg_url=url,
        driver_pg_connection_pool_min_size=10,
        driver_pg_connection_pool_max_size=10,
        driver_pg_connection_pool_acquire_timeout_ms=200,
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
            await cur.execute("TRUNCATE table resources")
            await cur.execute("TRUNCATE table catalog")

        driver = PGDriver(
            url=url,
            connection_pool_min_size=pg_maindb_settings.driver_pg_connection_pool_min_size,
            connection_pool_max_size=pg_maindb_settings.driver_pg_connection_pool_max_size,
            acquire_timeout_ms=pg_maindb_settings.driver_pg_connection_pool_acquire_timeout_ms,
        )
        await driver.initialize()
        await run_pg_schema_migrations(driver)

        yield driver

        await driver.finalize()
