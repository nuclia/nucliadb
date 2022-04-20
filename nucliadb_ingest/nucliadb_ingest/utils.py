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
from typing import Optional

from nucliadb_ingest.maindb.driver import Driver
from nucliadb_ingest.settings import settings
from nucliadb_utils.store import MAIN

try:
    from nucliadb_ingest.maindb.redis import RedisDriver

    REDIS = True
except ImportError:
    REDIS = False

try:
    from nucliadb_ingest.maindb.tikv import TiKVDriver

    TIKV = True
except ImportError:
    TIKV = False


try:
    from nucliadb_ingest.maindb.local import LocalDriver

    FILES = True
except ImportError:
    FILES = False


async def get_driver() -> Driver:
    if (
        settings.driver == "redis"
        and REDIS
        and "driver" not in MAIN
        and settings.driver_redis_url is not None
    ):
        redis_driver = RedisDriver(settings.driver_redis_url)
        MAIN["driver"] = redis_driver
    elif (
        settings.driver == "tikv"
        and TIKV
        and "driver" not in MAIN
        and settings.driver_tikv_url is not None
    ):
        tikv_driver = TiKVDriver(settings.driver_tikv_url)
        MAIN["driver"] = tikv_driver
    elif (
        settings.driver == "local"
        and FILES
        and "local" not in MAIN
        and settings.driver_local_url is not None
    ):
        local_driver = LocalDriver(settings.driver_local_url)
        MAIN["driver"] = local_driver
    driver: Optional[Driver] = MAIN.get("driver")
    if driver is not None and not driver.initialized:
        await driver.initialize()
    elif driver is None:
        raise AttributeError()
    return driver
