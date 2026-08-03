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

"""Migration #48

Backfills the KB to the new orm tables only if the KB has not been backfilled yet.

"""

import logging

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.backfill_orm_tables import backfill_kb
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:

    if not await should_backfill_kb(kbid):
        logger.info("KB does not need to be backfilled", extra={"kbid": kbid})
        return

    await backfill_kb(kbid=kbid)


async def should_backfill_kb(kbid: str) -> bool:
    async with datamanagers.with_ro_transaction() as txn:
        if not await datamanagers.kb.kb_v2.exists_kb(txn, kbid=kbid):
            logger.warning(
                "KB should be backfilled, as it does not exist in the new orm tables",
                extra={"kbid": kbid},
            )
            return True
        resources_count_v1 = await datamanagers.resources.calculate_number_of_resources(txn, kbid=kbid)
        resources_count_v2 = await datamanagers.resources.resources_v2.calculate_number_of_resources(
            txn, kbid=kbid
        )
        if resources_count_v1 != resources_count_v2:
            logger.warning(
                "KB should be backfilled, as the number of resources in the new orm tables does not match the old ones",
                extra={
                    "kbid": kbid,
                    "resources_count_v1": resources_count_v1,
                    "resources_count_v2": resources_count_v2,
                },
            )
            return True
    return False
