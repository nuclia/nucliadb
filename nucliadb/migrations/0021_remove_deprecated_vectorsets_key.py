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

"""Migration #21

Remove the old vectorsets maindb key to be able to reuse it with new data. The
old key was: "/kbs/{kbid}/vectorsets"

"""
import logging

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.kb import KB_SLUGS_BASE
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    key = f"/kbs/{kbid}/vectorsets"
    async with context.kv_driver.transaction() as txn:
        logger.info(f"Removing vectorsets key: {key}")
        await txn.delete(key)
        await txn.commit()
