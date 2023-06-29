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
import types
from unittest.mock import patch

from nucliadb.migrator import migrator


def test_get_migrations():
    migrations = migrator.get_migrations()
    assert len(migrations) > 0
    assert migrations[0].version == 1
    assert migrations[0].module.__name__ == "migrations.0001_bootstrap"


def test_get_migration_with_filtering():
    with patch("nucliadb.migrator.utils.get_migration_modules") as mock:
        mock.return_value = [
            (types.ModuleType("m1"), 1),
            (types.ModuleType("m2"), 2),
            (types.ModuleType("m3"), 3),
            (types.ModuleType("m4"), 4),
        ]
        migrations = migrator.get_migrations(from_version=2, to_version=3)
        assert len(migrations) == 1
        assert migrations[0].version == 3
        assert migrations[0].module.__name__ == "m3"
