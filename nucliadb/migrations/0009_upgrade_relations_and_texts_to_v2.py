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

"""Upgrade relations and texts indices to v2.

Rollover migration to upgrade relations and texts indices to their v2. The new
versions are configured as the default for stable and experimental, so a
rollover will do the upgrade automatically.

"""

from nucliadb.common.cluster.rollover import rollover_kb_shards
from nucliadb.migrator.context import ExecutionContext


async def migrate(context: ExecutionContext) -> None:
    ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    await rollover_kb_shards(context, kbid)
