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
from warnings import warn

from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.exceptions import UnsetUtility
from nucliadb.ingest.settings import DriverConfig, settings
from nucliadb_utils.exceptions import ConfigurationError
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility

try:
    from nucliadb.common.maindb.local import LocalDriver

    FILES = True
except ImportError:  # pragma: no cover
    FILES = False

try:
    from nucliadb.common.maindb.pg import PGDriver

    PG = True
except ImportError:  # pragma: no cover
    PG = False


def get_driver() -> Driver:
    driver = get_utility(Utility.MAINDB_DRIVER)
    if driver is None:
        raise UnsetUtility("Driver is not configured")
    return driver


async def setup_driver() -> Driver:
    driver = get_utility(Utility.MAINDB_DRIVER)
    if driver is not None:
        return driver

    if settings.driver == DriverConfig.PG:
        if not PG:
            raise ConfigurationError("`psycopg` python package not installed.")
        if settings.driver_pg_url is None:
            raise ConfigurationError("No DRIVER_PG_URL env var defined.")
        pg_driver = PGDriver(
            url=settings.driver_pg_url,
            connection_pool_min_size=settings.driver_pg_connection_pool_min_size,
            connection_pool_max_size=settings.driver_pg_connection_pool_max_size,
            acquire_timeout_ms=settings.driver_pg_connection_pool_acquire_timeout_ms,
        )
        set_utility(Utility.MAINDB_DRIVER, pg_driver)
    elif settings.driver == DriverConfig.LOCAL:
        if not FILES:
            raise ConfigurationError("`aiofiles` python package not installed.")
        if settings.driver_local_url is None:
            raise ConfigurationError("No DRIVER_LOCAL_URL env var defined.")
        local_driver = LocalDriver(settings.driver_local_url)
        set_utility(Utility.MAINDB_DRIVER, local_driver)
        warn("LocalDriver is not recommended for production use", RuntimeWarning)
    else:
        raise ConfigurationError(f"Invalid DRIVER defined configured: {settings.driver}")

    driver = get_driver()
    if not driver.initialized:
        await driver.initialize()
    return driver


async def teardown_driver() -> None:
    driver = get_utility(Utility.MAINDB_DRIVER)
    if driver is not None:
        if driver.initialized:
            await driver.finalize()
        clean_utility(Utility.MAINDB_DRIVER)
