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
from __future__ import annotations

import asyncio
import os
import uuid
from typing import List, Optional

from nucliadb_cluster.cluster import Cluster, Member
from nucliadb_ingest import logger
from nucliadb_ingest.orm.node import swim_reset, swim_update_node
from nucliadb_ingest.sentry import SENTRY
from nucliadb_ingest.settings import settings
from nucliadb_swim.member import NodeType
from nucliadb_swim.protocol import SocketAddr

if SENTRY:
    from sentry_sdk import capture_exception


def start_swim() -> Optional[SwimNucliaDBIngest]:
    if settings.swim_enabled is False:
        return None

    if os.path.exists(settings.swim_host_key):
        with open(settings.swim_host_key, "r") as f:
            key = f.read()
    else:
        key = uuid.uuid4().hex
        with open(settings.swim_host_key, "w+") as f:
            f.write(key)

    swim = SwimNucliaDBIngest(key)
    swim.start()

    return swim


class SwimNucliaDBIngest:
    cluster: Optional[Cluster] = None
    members_task: Optional[asyncio.Task] = None
    cluster_task: Optional[asyncio.Task] = None

    def __init__(self, key: str):
        self.key = key
        self.members_task = None
        self.cluster_task = None

    def start(self):
        # Cluster
        self.cluster = Cluster(
            self.key,
            SocketAddr(f"{settings.swim_binding_host}:{settings.swim_binding_port}"),
            NodeType.Writer,
            ping_interval=settings.swim_interval,
            ping_timeout=settings.swim_timeout,
            peers=[SocketAddr(x) for x in settings.swim_peers_addr],
        )

        self.cluster_task = asyncio.create_task(self.cluster.start(), name="SWIM")
        self.members_task = asyncio.create_task(self.wait(), name="MEMBERS")

    async def wait(self):
        while True:
            items: List[Member] = await self.cluster.members.get()
            try:
                await swim_update_node(items)
            except Exception as e:
                if SENTRY:
                    capture_exception(e)
                logger.exception("Exception on swim", exc_info=e)

    async def close(self):
        await swim_reset()
        await self.cluster.leave()
        await self.cluster.exit()
        self.cluster_task.cancel()
        self.members_task.cancel()
