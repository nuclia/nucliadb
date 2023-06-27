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
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.settings import settings
from nucliadb_utils.exceptions import ConfigurationError
from nucliadb_utils.store import MAIN

try:
    from nucliadb.common.maindb.redis import RedisDriver

    REDIS = True
except ImportError:  # pragma: no cover
    REDIS = False

try:
    from nucliadb.common.maindb.tikv import TiKVDriver

    TIKV = True
except ImportError:  # pragma: no cover
    TIKV = False


try:
    from nucliadb.common.maindb.pg import PGDriver

    PG = True
except ImportError:  # pragma: no cover
    PG = False

try:
    from nucliadb.common.maindb.local import LocalDriver

    FILES = True
except ImportError:  # pragma: no cover
    FILES = False

_DRIVER_UTIL_NAME = "driver"


def get_driver() -> Driver:
    return MAIN[_DRIVER_UTIL_NAME]


async def setup_driver() -> Driver:
    if _DRIVER_UTIL_NAME in MAIN:
        return MAIN[_DRIVER_UTIL_NAME]

    if settings.driver == "redis":
        if not REDIS:
            raise ConfigurationError("`redis` python package not installed.")
        if settings.driver_redis_url is None:
            raise ConfigurationError("No DRIVER_REDIS_URL env var defined.")

        redis_driver = RedisDriver(settings.driver_redis_url)
        MAIN[_DRIVER_UTIL_NAME] = redis_driver
    elif settings.driver == "tikv":
        if not TIKV:
            raise ConfigurationError("`tikv_client` python package not installed.")
        if settings.driver_tikv_url is None:
            raise ConfigurationError("No DRIVER_TIKV_URL env var defined.")

        tikv_driver = TiKVDriver(settings.driver_tikv_url)
        MAIN[_DRIVER_UTIL_NAME] = tikv_driver
    elif settings.driver == "pg":
        if not PG:
            raise ConfigurationError("`asyncpg` python package not installed.")
        if settings.driver_pg_url is None:
            raise ConfigurationError("No DRIVER_PG_URL env var defined.")
        pg_driver = PGDriver(settings.driver_pg_url)
        MAIN[_DRIVER_UTIL_NAME] = pg_driver
    elif settings.driver == "local":
        if not FILES:
            raise ConfigurationError("`aiofiles` python package not installed.")
        if settings.driver_local_url is None:
            raise ConfigurationError("No DRIVER_LOCAL_URL env var defined.")

        local_driver = LocalDriver(settings.driver_local_url)
        MAIN[_DRIVER_UTIL_NAME] = local_driver
    else:
        raise ConfigurationError(
            f"Invalid DRIVER defined configured: {settings.driver}"
        )

    driver: Driver = MAIN[_DRIVER_UTIL_NAME]
    if not driver.initialized:
        await driver.initialize()
    return driver


async def teardown_driver() -> None:
    if _DRIVER_UTIL_NAME in MAIN:
        driver: Driver = MAIN[_DRIVER_UTIL_NAME]
        if driver.initialized:
            await driver.finalize()
        del MAIN[_DRIVER_UTIL_NAME]
