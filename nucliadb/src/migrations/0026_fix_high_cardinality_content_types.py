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

"""Migration #26

Previously, there was no validation on content types added by users on upload. This caused that in some KBs,
there were content types that included random uuids, which caused high cardinality in the content type field.

This migration will fix those invalid content types.
"""

import logging

from nucliadb.common import datamanagers
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


AFFECTED_KBS = [
    "78d289e0-dd4d-448c-84b5-8ef0b01a5aba",
]


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    if kbid not in AFFECTED_KBS:
        return
    async for rid in datamanagers.resources.iterate_resource_ids(kbid=kbid):
        async with datamanagers.with_rw_transaction() as txn:
            basic = await datamanagers.resources.get_basic(txn, kbid=kbid, rid=rid)
            if not basic or not basic.icon:
                continue
            # We're aiming to fix content types like "multipart/form-data; boundary={uuid}"
            if "multipart/form-data" not in basic.icon:
                continue
            if "boundary=" not in basic.icon:
                continue
            logger.info("Fixing content type for resource", extra={"kbid": kbid, "rid": rid})
            basic.icon = "multipart/form-data"
            await datamanagers.resources.set_basic(txn, kbid=kbid, rid=rid, basic=basic)
            await txn.commit()
