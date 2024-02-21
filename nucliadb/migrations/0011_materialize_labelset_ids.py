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

"""Migration #11

Tikv doesn't really like scanning a lot of keys, so we need to materialize the labelset ids.

"""

import logging

from nucliadb.common.datamanagers.labels import LabelsDataManager
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None:
    ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    async with context.kv_driver.transaction() as txn:
        labelset_list = await LabelsDataManager._get_labelset_ids(kbid, txn)
        if labelset_list is not None:
            logger.info("No need for labelset list migration", extra={"kbid": kbid})
            return

        labelset_list = await LabelsDataManager._deprecated_scan_labelset_ids(kbid, txn)
        await LabelsDataManager._set_labelset_ids(kbid, txn, labelset_list)
        logger.info("Labelset list migrated", extra={"kbid": kbid})
        await txn.commit()
