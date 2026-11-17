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

"""Migration #45

Replaces deprecated and removed generative models from search configurations.

Mirrors the following learning_config migrations:
- ef4a9954aac5: claude-4-sonnet -> claude-4-6-sonnet

"""

from nucliadb.migrator.context import ExecutionContext
from nucliadb.migrator.search_configurations import migrate_search_configurations

REPLACEMENTS = {
    "claude-4-sonnet": "claude-4-6-sonnet",
}


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    await migrate_search_configurations(kbid, REPLACEMENTS)
