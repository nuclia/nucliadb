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
from nucliadb_ingest.orm.node import ClusterMember, chitchat_update_node
from nucliadb_ingest.sentry import SENTRY
from nucliadb_ingest.settings import settings

if SENTRY:
    from sentry_sdk import capture_exception


def start_chitchat() -> Optional[ChitchatNucliaDBIngest]:
    if settings.chitchat_enabled is False:
        return None

    while os.path.exists(settings.chitchat_sock_path) is False:
        sleep(0.1)

    chitchat = ChitchatNucliaDBIngest(settings.chitchat_sock_path)
    chitchat.start()

    return chitchat


class ChitchatNucliaDBIngest:
    update_task: Optional[asyncio.Task] = None

    def __init__(self, sock_path: str):
        self.sock_path = sock_path
        self.update_task = None

    def start(self):
        loop = asyncio.get_event_loop()
        self.update_task = asyncio.start_unix_server(
            self.socket_reader, path=self.sock_path
        )
        loop.run_until_complete(self.update_task)

    async def socket_reader(self, reader: asyncio.StreamReader, _):
        while True:
            try:
                update_readed = await reader.read(512)
                members: List[ClusterMember] = json.loads(
                    update_readed.decode("utf8").replace("'", '"')
                )
                if len(members) != 0:
                    await chitchat_update_node(members)
                else:
                    print("connection closed by writer")
                    break
            except IOError as e:
                if SENTRY:
                    capture_exception(e)
                logger.exception(f"error while reading update from unix socket: {e}")

    async def close(self):  # TODO ask where it's used
        # await chitchat_reset()
        self.members_task.cancel()
        self.update_task.cancel()
