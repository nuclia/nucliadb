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

"""Migration #31

At some point we had a bug that allowed to have repeated languages in the metadata basic.
This migration aims to fix that by removing the duplicates.

"""

import logging

from nucliadb.common import datamanagers
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    async with datamanagers.with_ro_transaction() as rs_txn:
        async for rid in datamanagers.resources.iterate_resource_ids(kbid=kbid):
            basic = await datamanagers.resources.get_basic(rs_txn, kbid=kbid, rid=rid)
            if basic is None:
                continue
            unique_langs = set(basic.metadata.languages)
            if len(unique_langs) != len(basic.metadata.languages):
                await fix_resource(kbid=kbid, rid=rid)


async def fix_resource(kbid: str, rid: str):
    async with datamanagers.with_rw_transaction() as txn:
        basic = await datamanagers.resources.get_basic(txn, kbid=kbid, rid=rid)
        if basic is None:
            return
        logger.info(f"Fixing duplicate languages", extra={"kbid": kbid, "rid": rid})
        unique_langs = set(basic.metadata.languages)
        basic.metadata.ClearField("languages")
        basic.metadata.languages.extend(list(unique_langs))
        await datamanagers.resources.set_basic(txn, kbid=kbid, rid=rid, basic=basic)
        await txn.commit()
