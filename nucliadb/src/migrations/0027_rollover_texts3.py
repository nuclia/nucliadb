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

"""Migration #27

Rollover for nucliadb_texts3
"""

import logging

from nucliadb import learning_proxy
from nucliadb.common import datamanagers
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    await maybe_fix_vector_dimensions(context, kbid)

    # We only need 1 rollover migration defined at a time; otherwise, we will
    # possibly run many for a kb when we only ever need to run one
    # await rollover_kb_index(context, kbid)


async def maybe_fix_vector_dimensions(context: ExecutionContext, kbid: str) -> None:
    learning_config = await learning_proxy.get_configuration(kbid)
    if learning_config is None:
        logger.warning(f"KB has no learning config", extra={"kbid": kbid})
        return

    async with context.kv_driver.rw_transaction() as txn:
        vectorsets = [vs async for vs in datamanagers.vectorsets.iter(txn, kbid=kbid)]
        if len(vectorsets) != 1:
            # If multiple vectorsets, they are new shards created correctly, we can safely skip it
            logger.warning(f"KB has {len(vectorsets)} vectorsets, skipping...", extra={"kbid": kbid})
            return
        vectorset = vectorsets[0][1]

        # Correct value, skip
        if vectorset.vectorset_index_config.vector_dimension != 0:
            return

        learning_model_metadata = learning_config.into_semantic_model_metadata()
        logger.info(
            f"Fixing KB vectorset dimension",
            extra={
                "kbid": kbid,
                "from": vectorset.vectorset_index_config.vector_dimension,
                "to": learning_model_metadata.vector_dimension,
            },
        )
        vectorset.vectorset_index_config.vector_dimension = learning_model_metadata.vector_dimension

        await datamanagers.vectorsets.set(txn, kbid=kbid, config=vectorset)
