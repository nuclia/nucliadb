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
from nucliadb.common.maindb.exceptions import UnsetUtility
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.ingest.settings import DriverConfig, settings
from nucliadb_utils.exceptions import ConfigurationError
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


def get_driver() -> Driver:
    driver = get_utility(Utility.MAINDB_DRIVER)
    if driver is None:
        raise UnsetUtility("Driver is not configured")
    return driver


async def setup_driver() -> Driver:
    driver = get_utility(Utility.MAINDB_DRIVER)
    if driver is not None:
        return driver

    if settings.driver == DriverConfig.MARKLOGIC:
        from nucliadb.common.marklogic.settings import settings as server_settings

        marklogic_driver = MarkLogicDriver(
            uri=server_settings.uri,
            username=server_settings.username or "",
            password=server_settings.password.get_secret_value() if server_settings.password else "",
            port=server_settings.system_port,
            admin_port=server_settings.admin_port,
            preallocated_partitions=server_settings.preallocated_partitions,
            replication=server_settings.replication,
        )
        set_utility(Utility.MAINDB_DRIVER, marklogic_driver)
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
