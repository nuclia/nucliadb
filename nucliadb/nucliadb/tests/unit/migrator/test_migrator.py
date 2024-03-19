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
from unittest.mock import AsyncMock, Mock, patch

import pytest

from nucliadb.migrator import command, migrator
from nucliadb.migrator.exceptions import MigrationValidationError
from nucliadb.migrator.models import Migration


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


async def test_run_all_kb_migrations_raises_on_failure():
    execution_context = Mock()
    execution_context.data_manager = Mock()
    execution_context.data_manager.get_kb_migrations = AsyncMock(
        return_value=["foo", "bar"]
    )
    execution_context.settings = Mock(max_concurrent_migrations=1)
    with patch(
        "nucliadb.migrator.migrator.run_kb_migrations",
        side_effect=[None, Exception("Boom")],
    ) as mock:
        with pytest.raises(Exception) as exc_info:
            await migrator.run_all_kb_migrations(execution_context, 1)
        assert "Failed to migrate KBs. Failures: 1" in str(exc_info.value)
        assert mock.call_count == 2


async def test_migrations_validation():
    migrations = [
        Migration(version=1, module=Mock()),
        Migration(version=2, module=Mock()),
        Migration(version=3, module=Mock()),
    ]
    with patch("nucliadb.migrator.command.get_migrations", return_value=migrations):
        command.validate()


async def test_migrations_validation_with_errors():
    migrations = [
        Migration(version=1, module=Mock()),
        Migration(version=2, module=Mock()),
        Migration(version=2, module=Mock()),
        Migration(version=3, module=Mock()),
    ]
    with patch("nucliadb.migrator.command.get_migrations", return_value=migrations):
        with pytest.raises(MigrationValidationError):
            command.validate()
