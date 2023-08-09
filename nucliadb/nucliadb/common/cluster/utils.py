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
from typing import Union

from nucliadb.common.cluster.discovery.utils import (
    setup_cluster_discovery,
    teardown_cluster_discovery,
)
from nucliadb.common.cluster.manager import KBShardManager, StandaloneKBShardManager
from nucliadb.common.cluster.settings import settings
from nucliadb.common.cluster.standalone.service import (
    start_grpc as start_standalone_grpc,
)
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility

_lock = asyncio.Lock()

_STANDALONE_SERVER = "_standalone_service"


async def setup_cluster() -> Union[KBShardManager, StandaloneKBShardManager]:
    async with _lock:
        if get_utility(Utility.SHARD_MANAGER) is not None:
            # already setup
            return get_utility(Utility.SHARD_MANAGER)

        await setup_cluster_discovery()
        mng: Union[KBShardManager, StandaloneKBShardManager]
        if settings.standalone_mode:
            server = await start_standalone_grpc()
            set_utility(_STANDALONE_SERVER, server)
            mng = StandaloneKBShardManager()
        else:
            mng = KBShardManager()
        set_utility(Utility.SHARD_MANAGER, mng)
        return mng


async def teardown_cluster():
    await teardown_cluster_discovery()
    if get_utility(Utility.SHARD_MANAGER):
        clean_utility(Utility.SHARD_MANAGER)

    std_server = get_utility(_STANDALONE_SERVER)
    if std_server is not None:
        await std_server.stop(None)
        clean_utility(_STANDALONE_SERVER)


def get_shard_manager() -> KBShardManager:
    return get_utility(Utility.SHARD_MANAGER)  # type: ignore
