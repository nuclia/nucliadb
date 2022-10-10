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
import binascii
import json
from typing import List, Optional

from nucliadb.ingest import logger
from nucliadb.ingest.orm.node import ClusterMember, chitchat_update_node
from nucliadb.ingest.settings import settings
from nucliadb.sentry import SENTRY
from nucliadb_utils.utilities import Utility, set_utility

if SENTRY:
    from sentry_sdk import capture_exception


def start_chitchat(service_name: str) -> Optional[ChitchatNucliaDB]:

    if settings.chitchat_enabled is False:
        logger.info(f"Chitchat not enabled - {service_name}")
        return None

    chitchat = ChitchatNucliaDB(
        settings.chitchat_binding_host, settings.chitchat_binding_port
    )
    asyncio.create_task(chitchat.start())
    logger.info("Chitchat started")
    set_utility(Utility.CHITCHAT, chitchat)

    return chitchat


class ChitchatNucliaDB:
    chitchat_update_srv: Optional[asyncio.Server] = None

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.chitchat_update_srv = None

    async def start(self):
        logger.info(f"enter chitchat.start() at {self.host}:{self.port}")
        self.chitchat_update_srv = await asyncio.start_server(
            self.socket_reader, host=self.host, port=self.port
        )
        logger.info(f"tcp server created")
        async with self.chitchat_update_srv:
            logger.info("awaiting connections from rust part of cluster")
            await asyncio.create_task(self.chitchat_update_srv.serve_forever())

    async def socket_reader(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        logger.info("new connection accepted")
        while True:
            try:
                logger.info("wait data in socket")
                mgr_message = await reader.read(
                    4096
                )  # TODO: add message types enum with proper deserialization
                if len(mgr_message) == 0:
                    logger.info("empty message received")
                    # TODO: Improve communication
                    await asyncio.sleep(1)
                    continue
                if len(mgr_message) == 4:
                    logger.info("check message received: {}".format(mgr_message.hex()))
                    hash = binascii.crc32(mgr_message)
                    logger.info(f"calculated hash: {hash}")
                    response = hash.to_bytes(4, byteorder="big")
                    logger.info("Hash response: {!r}".format(response))
                    writer.write(response)
                    await writer.drain()
                    continue
                else:
                    logger.info("update message received: {!r}".format(mgr_message))
                    members: List[ClusterMember] = list(
                        map(
                            lambda x: ClusterMember(
                                node_id=x["node_id"],
                                listen_addr=x["listen_addr"],
                                node_type=x["node_type"],
                                is_self=x["is_self"],
                                online=True,
                            ),
                            json.loads(mgr_message.decode("utf8").replace("'", '"')),
                        )
                    )
                    logger.info(f"updated members: {members}")
                    if len(members) != 0:
                        await chitchat_update_node(members)
                        writer.write(len(members).to_bytes(4, byteorder="big"))
                        await writer.drain()
                    else:
                        logger.info("connection closed by writer")
                        break
            except IOError as e:
                logger.exception("Failed on chitchat", stack_info=True)
                if SENTRY:
                    capture_exception(e)
                logger.exception(f"error while reading update from unix socket: {e}")

    async def close(self):
        self.chitchat_update_srv.close()
