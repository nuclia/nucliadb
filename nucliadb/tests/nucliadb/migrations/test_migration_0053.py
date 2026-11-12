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

import pytest

from nucliadb.common.maindb.driver import Driver
from nucliadb.migrator.models import Migration
from tests.nucliadb.migrations import get_migration

from .utils import _test_model_deprecation_migration

migration: Migration = get_migration(53)


@pytest.mark.parametrize("deprecated,replacement", [("aws-claude-4-1-opus", "aws-claude-4-8-opus")])
async def test_migration_0053(maindb_driver: Driver, deprecated: str, replacement: str):
    await _test_model_deprecation_migration(
        maindb_driver, migration, deprecated_model=deprecated, replacement_model=replacement
    )
