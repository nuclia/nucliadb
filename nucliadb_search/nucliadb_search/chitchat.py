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
import json
import os
from time import sleep
from typing import List, Optional

from nucliadb_ingest import logger
from nucliadb_ingest.orm.node import (
    ClusterMember,
    DefinedNodesNucliaDBSearch,
    chitchat_update_node,
)
from nucliadb_ingest.sentry import SENTRY
from nucliadb_search.settings import settings

if SENTRY:
    from sentry_sdk import capture_exception


def start_chitchat() -> Optional[ChitchatNucliaDBSearch]:
    if settings.nodes_load_ingest:
        # used for testing proposes
        load_nodes = DefinedNodesNucliaDBSearch()
        asyncio.create_task(load_nodes.start(), name="NODES_LOAD")

    if settings.chitchat_enabled is False:
        return None

    chitchat = ChitchatNucliaDBSearch(
        settings.chitchat_binding_host, settings.chitchat_binding_port
    )
    asyncio.create_task(chitchat.start())

    return chitchat


class ChitchatNucliaDBSearch:
    chitchat_update_srv: Optional[asyncio.Task] = None

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.chitchat_update_srv = None

    async def start(self):
        print("enter chitchat.start()")
        self.chitchat_update_srv = await asyncio.start_server(
            self.socket_reader, host=self.host, port=self.port
        )
        print("tcp server created ")
        async with self.chitchat_update_srv:
            print("awaiting connections from rust part of cluster")
            await asyncio.create_task(self.chitchat_update_srv.serve_forever())

    async def socket_reader(self, reader: asyncio.StreamReader, _):
        print("new connection accepted")
        while True:
            try:
                print("wait data in socket")
                update_readed = await reader.read()
                print(f"data readed {update_readed}")
                members: List[ClusterMember] = json.loads(
                    update_readed.decode("utf8").replace("'", '"')
                )
                if len(members) != 0:
                    print(f"members: {members}")
                    await chitchat_update_node(members)
                else:
                    print("connection closed by writer")
                    break
            except IOError as e:
                print("exception")
                if SENTRY:
                    capture_exception(e)
                logger.exception(f"error while reading update from unix socket: {e}")

    async def close(self):
        self.chitchat_update_srv.cancel()
