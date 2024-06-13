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

"""Migration #15

Targeted rollover for a specific KB
"""

import logging
import os

from nucliadb.common.cluster.rollover import rollover_kb_shards
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


AFFECTED_KBS = [kbid.strip() for kbid in os.environ.get("ROLLOVER_KBS", "").split(",") if kbid.strip()]


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    if kbid in AFFECTED_KBS:
        logger.info(f"Rolling over affected KB: {kbid}")
        await rollover_kb_shards(context, kbid)
