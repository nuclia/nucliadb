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

from nucliadb.common.cluster.manager import (
    KBShardManager,
    StandaloneKBShardManager,
)
from nucliadb.common.cluster.settings import settings
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility

_lock = asyncio.Lock()


async def setup_cluster() -> KBShardManager | StandaloneKBShardManager:
    async with _lock:
        if get_utility(Utility.SHARD_MANAGER) is not None:
            # already setup
            return get_utility(Utility.SHARD_MANAGER)

        mng: KBShardManager | StandaloneKBShardManager
        if settings.standalone_mode:
            mng = StandaloneKBShardManager()
        else:
            mng = KBShardManager()
        set_utility(Utility.SHARD_MANAGER, mng)
        return mng


async def teardown_cluster():
    if get_utility(Utility.SHARD_MANAGER):
        clean_utility(Utility.SHARD_MANAGER)


def get_shard_manager() -> KBShardManager:
    return get_utility(Utility.SHARD_MANAGER)


async def get_nats_consumer_pending_messages(
    nats_manager: NatsConnectionManager, *, stream: str, consumer: str
) -> int:
    # get raw js client
    js = nats_manager.js
    consumer_info = await js.consumer_info(stream, consumer)
    return consumer_info.num_pending


async def wait_for_indexing() -> None:
    # TODO(Marklogic): Wait for backend indexing to catch up.
    return None


async def wait_for_nidx(
    nats_manager: NatsConnectionManager,
    max_pending: int,
    poll_interval_seconds: int = 5,
    max_wait_seconds: int = 60,
):
    await wait_for_indexing()
