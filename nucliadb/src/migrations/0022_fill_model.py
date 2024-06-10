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

"""Migration #22

Fill the model metadata in the shards key of some old KBs that may not have it.

"""
import logging

from nucliadb import learning_proxy
from nucliadb.common import datamanagers
from nucliadb.ingest.service.writer import parse_model_metadata_from_learning_config
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    if not await needs_fixing(kbid):
        logger.info("Model metadata already filled", extra={"kbid": kbid})
        return

    # Get learning config
    learning_config = await learning_proxy.get_configuration(kbid)
    if learning_config is None:
        logger.error("KB has no learning configuration", extra={"kbid": kbid})
        return

    model_metadata = parse_model_metadata_from_learning_config(learning_config)
    if model_metadata.vector_dimension is None:
        logger.error(
            "Vector dimension not set in learning config", extra={"kbid": kbid}
        )
        return

    async with datamanagers.with_rw_transaction() as txn:
        shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if shards is None:
            logger.error("KB has no shards", extra={"kbid": kbid})
            return

        shards.model.CopyFrom(model_metadata)
        await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=shards)
        await txn.commit()

    logger.info("Model metadata filled", extra={"kbid": kbid})


async def needs_fixing(kbid: str) -> bool:
    async with datamanagers.with_ro_transaction() as txn:
        shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if shards is None:
            logger.error("KB has no shards", extra={"kbid": kbid})
            return False

        return shards.model is None or shards.model.vector_dimension == 0
