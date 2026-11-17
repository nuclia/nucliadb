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
# Copyright (C) 2021 Bosutech XXI S.L.

"""Migration #52

Replaces deprecated generative models from search configurations.
"""

from nucliadb.migrator.context import ExecutionContext
from nucliadb.migrator.search_configurations import migrate_search_configurations

REPLACEMENTS = {
    "claude-4-5-sonnet": "claude-5-sonnet",
    "chatgpt-azure-4o": "chatgpt-azure-5.6-terra",
    "chatgpt-azure-4o-mini": "chatgpt-azure-5.6-luna",
    "chatgpt-azure-o3-mini": "chatgpt-azure-5.6-luna",
}


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    await migrate_search_configurations(kbid, REPLACEMENTS)
