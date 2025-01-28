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
import asyncio
import logging
from typing import TYPE_CHECKING, Optional, Union

import backoff

from nucliadb.common import datamanagers
from nucliadb.common.cluster.manager import (
    KBShardManager,
    StandaloneKBShardManager,
)
from nucliadb.common.cluster.settings import settings
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import nodereader_pb2, writer_pb2
from nucliadb_utils import const
from nucliadb_utils.settings import is_onprem_nucliadb
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility

if TYPE_CHECKING:  # pragma: no cover
    from nucliadb.common.context import ApplicationContext
else:
    ApplicationContext = None

logger = logging.getLogger(__name__)


_lock = asyncio.Lock()

_STANDALONE_SERVER = "_standalone_service"


async def setup_cluster() -> Union[KBShardManager, StandaloneKBShardManager]:
    async with _lock:
        if get_utility(Utility.SHARD_MANAGER) is not None:
            # already setup
            return get_utility(Utility.SHARD_MANAGER)

        mng: Union[KBShardManager, StandaloneKBShardManager]
        if settings.standalone_mode:
            mng = StandaloneKBShardManager()
        else:
            mng = KBShardManager()
        set_utility(Utility.SHARD_MANAGER, mng)
        return mng


async def teardown_cluster():
    if get_utility(Utility.SHARD_MANAGER):
        clean_utility(Utility.SHARD_MANAGER)

    std_server = get_utility(_STANDALONE_SERVER)
    if std_server is not None:
        await std_server.stop(None)
        clean_utility(_STANDALONE_SERVER)


def get_shard_manager() -> KBShardManager:
    return get_utility(Utility.SHARD_MANAGER)  # type: ignore


async def wait_for_node(app_context: ApplicationContext, node_id: str) -> None:
    if is_onprem_nucliadb():
        # On onprem deployments indexing is synchronous right now, so we don't need to wait
        return

    logged = False
    while True:
        # get raw js client
        js = app_context.nats_manager.js
        consumer_info = await js.consumer_info(
            const.Streams.INDEX.name, const.Streams.INDEX.group.format(node=node_id)
        )
        if consumer_info.num_pending < 5:
            return

        if not logged:
            logger.info(
                f"Waiting for node to consume messages. {consumer_info.num_pending} messages left.",
                extra={"node": node_id},
            )
            logged = True
        # usually we consume around 3-4 messages/s with some eventual peaks of
        # 10-30. If there are too many pending messages, we can wait more.
        # We suppose 5 messages/s and don't wait more than 60s
        sleep = min(max(2, consumer_info.num_pending / 5), 60)
        await asyncio.sleep(sleep)


async def get_resource(kbid: str, resource_id: str) -> Optional[Resource]:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.resources.get_resource(txn, kbid=kbid, rid=resource_id)


@backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=8)
async def get_resource_index_message(kbid: str, resource_id: str) -> Optional[nodereader_pb2.Resource]:
    async with datamanagers.with_ro_transaction() as txn:
        resource = await datamanagers.resources.get_resource(txn, kbid=kbid, rid=resource_id)
        if resource is None:
            logger.warning(
                "Resource not found while indexing, skipping",
                extra={"kbid": kbid, "resource_id": resource_id},
            )
            return None
        resource_index_message = (await resource.generate_index_message(reindex=False)).brain
        return resource_index_message


@backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=8)
async def index_resource_to_shard(
    app_context: ApplicationContext,
    kbid: str,
    resource_id: str,
    shard: writer_pb2.ShardObject,
    resource_index_message: Optional[nodereader_pb2.Resource] = None,
) -> None:
    logger.info("Indexing resource", extra={"kbid": kbid, "resource_id": resource_id})
    sm = app_context.shard_manager
    partitioning = app_context.partitioning

    if resource_index_message is None:
        resource_index_message = await get_resource_index_message(kbid, resource_id)
        if resource_index_message is None:
            return

    partition = partitioning.generate_partition(kbid, resource_id)
    await sm.add_resource(shard, resource_index_message, txid=-1, partition=str(partition), kb=kbid)


async def delete_resource_from_shard(
    app_context: ApplicationContext,
    kbid: str,
    resource_id: str,
    shard: writer_pb2.ShardObject,
) -> None:
    logger.info("Deleting resource", extra={"kbid": kbid, "resource_id": resource_id})

    sm = app_context.shard_manager
    partitioning = app_context.partitioning
    partition = partitioning.generate_partition(kbid, resource_id)

    await sm.delete_resource(shard, resource_id, 0, str(partition), kbid)
