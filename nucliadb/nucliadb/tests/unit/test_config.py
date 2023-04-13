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

from unittest.mock import patch

import pytest

from nucliadb.config import config_nucliadb
from nucliadb.settings import Settings


@pytest.fixture(scope="function")
def config_standalone_driver():
    with patch("nucliadb.config.config_standalone_driver") as mock:
        yield mock


def test_config_nucliadb_configures_standalone(config_standalone_driver):
    settings = Settings()
    config_nucliadb(settings)
    config_nucliadb(settings, standalone=False)

    config_standalone_driver.assert_not_called()

    config_nucliadb(settings, standalone=True)

    config_standalone_driver.assert_called_once_with(settings)
