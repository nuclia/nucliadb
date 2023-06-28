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
import types
from functools import lru_cache

import migrations

from .models import Migration

logger = logging.getLogger(__name__)

MIGRATION_DIR = os.path.sep.join(
    os.path.dirname(os.path.abspath(__file__)).split(os.path.sep)[:-2] + ["migrations"]
)


def get_migration_modules() -> list[tuple[types.ModuleType, int]]:
    output = []
    for filename in os.listdir(MIGRATION_DIR):
        if filename.endswith(".py") and filename != "__init__.py":
            module_name = filename[:-3]
            version = int(module_name.split("_")[0])
            __import__(f"migrations.{module_name}")
            module = getattr(migrations, module_name)
            if not hasattr(module, "migrate"):
                raise Exception(f"Missing `migrate` function in {module_name}")
            if not hasattr(module, "migrate_kb"):
                raise Exception(f"Missing `migrate_kb` function in {module_name}")
            output.append((module, version))
    return output


def get_migrations(from_version: int = 0, to_version: int = 99999999):
    migrations: list[Migration] = []
    for module, version in get_migration_modules():
        migrations.append(Migration(version=version, module=module))

    migrations.sort(key=lambda m: m.version)
    return [
        m for m in migrations if m.version > from_version and m.version <= to_version
    ]


@lru_cache(maxsize=None)
def get_latest_version() -> int:
    return get_migrations()[-1].version
