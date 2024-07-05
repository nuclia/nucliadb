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

"""Migration #18

Due to a bug on backend services, some kbslugs were not properly deleted and got
orphan. Let's delete them!

"""

import logging

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.kb import KB_SLUGS_BASE
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None:
    async with context.kv_driver.transaction() as txn:
        async for key in txn.keys(KB_SLUGS_BASE, count=-1):
            slug = key.replace(KB_SLUGS_BASE, "")
            value = await txn.get(key, for_update=False)
            if value is None:
                # KB with slug but without uuid? Seems wrong, let's remove it too
                logger.info("Removing /kbslugs with empty value", extra={"maindb_key": key})
                await txn.delete(key)
                continue

            kbid = value.decode()
            if not (await datamanagers.kb.exists_kb(txn, kbid=kbid)):
                # log data too just in case
                logger.info(
                    "Removing orphan /kbslugs key",
                    extra={"kbid": kbid, "kb_slug": slug, "maindb_key": key},
                )
                await txn.delete(key)
        await txn.commit()


async def migrate_kb(context: ExecutionContext, kbid: str) -> None: ...
