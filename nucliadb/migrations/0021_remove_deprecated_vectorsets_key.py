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
import re

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.kb import KB_SLUGS_BASE
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


DEPRECATED_KEY_REGEX = re.compile(r"^/kbs/[a-zA-Z0-9-]+/vectorsets$")


async def migrate(context: ExecutionContext) -> None:
    keys_to_delete = []
    async with context.kv_driver.transaction(read_only=True) as txn:
        async for key in txn.keys("/kbs/", count=-1):
            if re.fullmatch(DEPRECATED_KEY_REGEX, key):
                keys_to_delete.append(key)

    batch_size = 50
    while keys_to_delete:
        async with context.kv_driver.transaction() as txn:
            batch = keys_to_delete[:batch_size]
            for key in batch:
                logger.info(f"Removing vectorsets key: {key}")
                await txn.delete(key)
            await txn.commit()
            del keys_to_delete[:batch_size]


async def migrate_kb(context: ExecutionContext, kbid: str) -> None: ...
