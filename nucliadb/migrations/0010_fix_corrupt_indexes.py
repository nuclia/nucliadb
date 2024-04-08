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

"""Migration #10

Due to a bug in the index nodes, some KBs have been affected in stage with an
index data loss. Rollover affected KBs

"""

import logging

from nucliadb.common.cluster.rollover import rollover_kb_shards
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)

AFFECTED_KBS = [
    "1efc5a33-bc5a-490c-8b47-b190beee212d",
    "f11d6eb9-da5e-4519-ac3d-e304bfa5c354",
    "096d9070-f7be-40c8-a24c-19c89072e3ff",
    "848f01bc-341a-4346-b473-6b11b76b26eb",
]


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    if kbid in AFFECTED_KBS:
        logger.info(f"Rolling over affected KB: {kbid}")
        await rollover_kb_shards(context, kbid)
