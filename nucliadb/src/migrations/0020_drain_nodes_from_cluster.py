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

"""Migration #20
This migration is for reducing the number of nodes in a cluster.
Essentially, it is a rollover shards migration only for KBs that have
shards in the nodes we want to remove from the cluster.
Will read the DRAIN_NODES envvar to get the list of nodes to drain, and will
create new shards in the remaining nodes.
"""

import logging

from nucliadb.common import datamanagers
from nucliadb.common.cluster.rollover import rollover_kb_shards
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    """
    Rollover KB shards if any of the shards are on the nodes to drain
    """
    drain_node_ids = cluster_settings.drain_nodes
    if len(drain_node_ids) == 0:
        logger.info("Skipping migration because no drain_nodes are set")
        return

    if not await kb_has_shards_on_drain_nodes(kbid, drain_node_ids):
        logger.info(
            "KB does not have shards on the nodes to drain, skipping rollover",
            extra={"kbid": kbid},
        )
        return

    logger.info("Rolling over affected KB", extra={"kbid": kbid})
    await rollover_kb_shards(context, kbid, drain_nodes=drain_node_ids)


async def kb_has_shards_on_drain_nodes(kbid: str, drain_node_ids: list[str]) -> bool:
    async with datamanagers.with_ro_transaction() as txn:
        shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if not shards:
            logger.warning("Shards object not found", extra={"kbid": kbid})
            return False
        shard_in_drain_nodes = False
        for shard in shards.shards:
            for replica in shard.replicas:
                if replica.node in drain_node_ids:
                    logger.info(
                        "Shard found in drain nodes, will rollover it",
                        extra={
                            "kbid": kbid,
                            "logical_shard": shard.shard,
                            "replica_shard_id": replica.shard.id,
                            "node": replica.node,
                            "drain_node_ids": drain_node_ids,
                        },
                    )
                    shard_in_drain_nodes = True
        return shard_in_drain_nodes
