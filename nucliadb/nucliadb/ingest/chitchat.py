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
from typing import Dict, List, Optional, Union

from nucliadb.ingest import logger
from nucliadb.ingest.orm.node import ClusterMember, Node, NodeType, chitchat_update_node
from nucliadb.ingest.settings import settings
from nucliadb_telemetry import errors
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


async def start_chitchat(service_name: str) -> Optional[ChitchatNucliaDB]:
    util = get_utility(Utility.CHITCHAT)
    if util is not None:
        # already loaded
        return util

    if settings.nodes_load_ingest:  # pragma: no cover
        await Node.load_active_nodes()
        return None

    if settings.chitchat_enabled is False:
        logger.debug(f"Chitchat not enabled - {service_name}")
        return None

    chitchat = ChitchatNucliaDB(
        settings.chitchat_binding_host, settings.chitchat_binding_port
    )
    await chitchat.start()
    logger.info("Chitchat started")
    set_utility(Utility.CHITCHAT, chitchat)

    return chitchat


async def stop_chitchat():
    util = get_utility(Utility.CHITCHAT)
    if util is not None:
        await util.close()
        clean_utility(Utility.CHITCHAT)


class ChitchatNucliaDB:
    chitchat_update_srv: Optional[asyncio.Server] = None

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.chitchat_update_srv = None
        self.task = None

    async def inner_start(self):
        async with self.chitchat_update_srv:
            logger.info("awaiting connections from rust part of cluster")
            await self.chitchat_update_srv.serve_forever()

    async def finalize(self):
        await self.close()

    async def start(self):
        logger.info(f"enter chitchat.start() at {self.host}:{self.port}")
        self.chitchat_update_srv = await asyncio.start_server(
            self.socket_reader, host=self.host, port=self.port
        )
        logger.info(f"tcp server created")
        self.task = asyncio.create_task(self.inner_start())

        await asyncio.sleep(0.1)

    async def socket_reader(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        peer = writer.get_extra_info("peername")
        logger.info(f"new connection accepted: {peer}")
        try:
            while True:
                logger.debug("wait data in socket")
                mgr_message = await reader.read(
                    4096
                )  # TODO: add message types enum with proper deserialization
                if len(mgr_message) == 0:
                    logger.warning(f"empty message received from {peer}. Disconnected")
                    break
                if len(mgr_message) == 4:
                    logger.debug(
                        f"check message received from {peer}: {mgr_message.hex()}"
                    )
                    hash = binascii.crc32(mgr_message)
                    response = hash.to_bytes(4, byteorder="big")
                    writer.write(response)
                    await writer.drain()
                    continue
                else:
                    logger.debug(
                        f"update message received from {peer}: {mgr_message!r}"
                    )
                    members: List[ClusterMember] = list(
                        map(
                            lambda x: build_member_from_json(x),
                            json.loads(mgr_message.decode("utf8").replace("'", '"')),
                        )
                    )
                    logger.debug(f"updated members: {members}")
                    if len(members) != 0:
                        await chitchat_update_node(members)
                        writer.write(len(members).to_bytes(4, byteorder="big"))
                        await writer.drain()
                    else:
                        logger.warning("connection closed by writer")
                        break
        except (
            KeyboardInterrupt,
            SystemExit,
            asyncio.CancelledError,
        ):  # pragma: no cover
            logger.info(f"Exiting chitchat connection with {peer}")
        except (IOError, BrokenPipeError) as e:
            logger.exception(
                f"Failed on chitchat connection with {peer}", stack_info=True
            )
            errors.capture_exception(e)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                logger.warning(
                    f"Errors closing writer connection with {peer}", exc_info=True
                )

    async def close(self):
        self.chitchat_update_srv.close()
        await self.chitchat_update_srv.wait_closed()
        self.task.cancel()


JsonValue = Union["JsonObject", list["JsonValue"], str, bool, int, float, None]
JsonArray = List[JsonValue]
JsonObject = Dict[str, JsonValue]


def parse_shard_count(member_serial: JsonObject) -> int:
    shard_count_str = member_serial.get("shard_count")
    if not shard_count_str:
        # shard_count is only set for type.IO nodes
        logger.debug("Missing shard_count: Defaulted to 0")
        return 0
    try:
        shard_count = int(shard_count_str)  # type: ignore
    except ValueError:
        logger.warning(
            f"Cannot convert shard_count ({shard_count_str}). Defaulted to 0"
        )
        shard_count = 0
    return shard_count


def build_member_from_json(member_serial: JsonObject):
    return ClusterMember(
        node_id=str(member_serial["id"]),
        listen_addr=str(member_serial["address"]),
        type=NodeType.from_str(member_serial["type"]),
        is_self=bool(member_serial["is_self"]),
        shard_count=parse_shard_count(member_serial),
    )


if __name__ == "__main__":  # pragma: no cover
    # run chitchat server locally without dependencies
    import logging

    logging.basicConfig(level=logging.DEBUG)

    async def run_forever():
        cc = await start_chitchat("test")
        await cc.task

    asyncio.run(run_forever())
