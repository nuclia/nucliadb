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

"""Migration #28

Add a key to each vectorset to know how to build the storage key for extracted vectors
"""

import logging

from nucliadb.common import datamanagers
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos import knowledgebox_pb2

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    async with datamanagers.with_rw_transaction() as txn:
        vectorsets = [vs async for (_vid, vs) in datamanagers.vectorsets.iter(txn, kbid=kbid)]

        if len(vectorsets) == 0:  # pragma: nocover
            # should never happen, everyone should have at least one
            logger.warning(f"KB has no vectorsets!", extra={"kbid": kbid})
            return

        elif len(vectorsets) == 1:
            logger.info(f"Migrating KB with a single vectorset", extra={"kbid": kbid})
            vectorset = vectorsets[0]
            vectorset.storage_key_kind = knowledgebox_pb2.VectorSetConfig.StorageKeyKind.LEGACY
            await datamanagers.vectorsets.set(txn, kbid=kbid, config=vectorset)

        else:
            logger.info(f"Migrating KB with {len(vectorsets)} vectorsets", extra={"kbid": kbid})
            for vectorset in vectorsets:
                vectorset.storage_key_kind = (
                    knowledgebox_pb2.VectorSetConfig.StorageKeyKind.VECTORSET_PREFIX
                )
                await datamanagers.vectorsets.set(txn, kbid=kbid, config=vectorset)

        await txn.commit()
