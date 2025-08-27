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

With the new vectorsets implementation, we need to store some information on
maindb. As the key "/kbs/{kbid}/vectorsets" was already used at some point, this
migration will ensure to overwrite the key and set the new value

"""

import logging

from nucliadb.common import datamanagers
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    async with context.kv_driver.rw_transaction() as txn:
        logger.info(f"Overwriting vectorsets key", extra={"kbid": kbid})
        await datamanagers.vectorsets.initialize(txn, kbid=kbid)
        await txn.commit()
