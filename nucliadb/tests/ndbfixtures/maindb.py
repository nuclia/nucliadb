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
import random
from typing import AsyncIterator, Iterator
from unittest.mock import patch
from urllib.parse import urlparse, urlunparse

import psycopg
import pytest
from pytest_docker_fixtures import images

from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.pg import PGDriver, PGTransaction
from nucliadb.ingest.settings import DriverConfig, DriverSettings
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb.migrator.migrator import run_pg_schema_migrations
from nucliadb_utils.utilities import Utility
from tests.ndbfixtures.utils import global_utility

logger = logging.getLogger("nucliadb.fixtures:maindb")

# Minimum support PostgreSQL version
# Reason: We want the btree_gin extension to support uuid's (pg11) and `gen_random_uuid()` (pg13)
images.settings["postgresql"]["version"] = "13"
images.settings["postgresql"]["env"]["POSTGRES_PASSWORD"] = "postgres"


@pytest.fixture(scope="function")
def maindb_settings(pg_maindb_settings) -> Iterator[DriverSettings]:
    yield pg_maindb_settings


@pytest.fixture(scope="function")
async def maindb_driver(pg_maindb_driver) -> AsyncIterator[Driver]:
    driver: Driver = pg_maindb_driver

    with global_utility(Utility.MAINDB_DRIVER, driver):
        yield driver

    try:
        await cleanup_maindb(driver)
    except Exception:
        logger.exception("Could not cleanup maindb on test teardown")


async def cleanup_maindb(driver: Driver):
    if not driver.initialized:
        return

    async with driver.ro_transaction() as txn:
        all_keys = [k async for k in txn.keys("")]

    async with driver.rw_transaction() as txn:
        for key in all_keys:
            await txn.delete(key)
        await txn.commit()

        try:
            if isinstance(txn, PGTransaction):
                async with txn.connection.cursor() as cur:
                    await cur.execute("TRUNCATE shards CASCADE")
        except Exception:
            pass


async def create_test_database(base_url):
    async with (
        await psycopg.AsyncConnection.connect(base_url, autocommit=True) as conn,
        conn.cursor() as cursor,
    ):
        for i in range(10):
            try:
                dbname = f"_nucliadb_test_{random.randint(0, 999999)}"
                await cursor.execute(f"CREATE DATABASE {dbname}")
                return dbname
            except psycopg.errors.DuplicateDatabase:
                pass


@pytest.fixture(scope="function")
async def pg_maindb_settings(request) -> AsyncIterator[DriverSettings]:
    if "TESTING_PG_URL" in os.environ.keys():
        # Connect to a running server and create a new DB
        base_url = os.environ["TESTING_PG_URL"]
        dbname = await create_test_database(base_url)
        parsed = urlparse(base_url)
        url = urlunparse(parsed._replace(path=f"/{dbname}"))
    else:
        # Start a new database server in a docker container
        pg = request.getfixturevalue("pg")
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
async def pg_maindb_driver(pg_maindb_settings: DriverSettings) -> AsyncIterator[PGDriver]:
    url = pg_maindb_settings.driver_pg_url
    assert url is not None
    with (
        patch.object(ingest_settings, "driver", DriverConfig.PG),
        patch.object(ingest_settings, "driver_pg_url", url),
    ):
        async with await psycopg.AsyncConnection.connect(url) as conn, conn.cursor() as cur:
            await cur.execute("TRUNCATE TABLE resources")
            await cur.execute("TRUNCATE TABLE catalog CASCADE")

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
