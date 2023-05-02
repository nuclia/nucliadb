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
from typing import Dict, List, Optional, Tuple

from fastapi import FastAPI, Response
from uvicorn.config import Config  # type: ignore
from uvicorn.server import Server  # type: ignore

from nucliadb.ingest import logger
from nucliadb.ingest.orm.node import NODES, Node
from nucliadb.ingest.settings import settings
from nucliadb_models.cluster import ClusterMember, MemberType
from nucliadb_telemetry import errors, metrics
from nucliadb_utils.fastapi.run import run_server_forever
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility

AVAILABLE_NODES = metrics.Gauge("nucliadb_nodes_available")

SHARD_COUNT = metrics.Gauge(
    "nucliadb_node_shard_count",
    labels={"node": ""},
)

LOAD_SCORE = metrics.Gauge(
    "nucliadb_node_load_score",
    labels={"node": ""},
)


async def start_chitchat(service_name: str) -> Optional[ChitchatMonitor]:
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

    chitchat = ChitchatMonitor(
        settings.chitchat_binding_host, settings.chitchat_binding_port
    )
    await chitchat.start()
    logger.info("Chitchat started")
    set_utility(Utility.CHITCHAT, chitchat)

    return chitchat


async def stop_chitchat():
    util = get_utility(Utility.CHITCHAT)
    if util is not None:
        await util.finalize()
        clean_utility(Utility.CHITCHAT)


chitchat_app = FastAPI(title="ChitChat")


@chitchat_app.patch("/members", status_code=204)
async def update_members(members: List[ClusterMember]) -> Response:
    await update_available_nodes(members)
    return Response(status_code=204)


def get_configured_chitchat_app(host: str, port: int) -> Tuple[Server, Config]:
    config = Config(
        chitchat_app,
        host=host,
        port=port,
        debug=False,
        loop="auto",
        http="auto",
        reload=False,
        workers=1,
        use_colors=False,
        log_level="warning",
        limit_concurrency=None,
        backlog=2047,
        limit_max_requests=None,
        timeout_keep_alive=5,
        access_log=False,
    )
    server = Server(config=config)
    return server, config


class ChitchatMonitor:
    """
    This is starting a HTTP server that will receives periodic chitchat-cluster
    member changes and it will update the in-memory list of available nodes.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.task = None

    async def start(self):
        logger.info("Chitchat server started at")
        server, config = get_configured_chitchat_app(self.host, self.port)
        self.task = asyncio.create_task(run_server_forever(server, config))

    async def finalize(self):
        logger.info("Chitchat closed")
        self.task.cancel()


async def update_available_nodes(members: List[ClusterMember]) -> None:
    # First add new nodes or update existing ones
    valid_ids = []
    for member in members:
        valid_ids.append(member.node_id)
        if member.is_self or member.type != MemberType.IO:
            continue

        node = NODES.get(member.node_id)
        if node is None:
            logger.debug(f"{member.node_id}/{member.type} add {member.listen_addr}")
            await Node.set(
                member.node_id,
                address=member.listen_addr,
                type=member.type,
                load_score=member.load_score,
                shard_count=member.shard_count,
            )
            logger.debug("Node added")
        else:
            logger.debug(f"{member.node_id}/{member.type} update")
            node.load_score = member.load_score
            node.shard_count = member.shard_count
            logger.debug("Node updated")

    # Then cleanup nodes that are no longer reported
    node_ids = [x for x in NODES.keys()]
    destroyed_node_ids = []
    for key in node_ids:
        if key not in valid_ids:
            node = NODES.get(key)
            if node is not None:
                destroyed_node_ids.append(key)
                logger.warning(f"{key}/{node.type} remove {node.address}")
                await Node.destroy(key)

    try:
        if len(destroyed_node_ids) > 1:
            raise Exception(
                f"{len(destroyed_node_ids)} nodes are down simultaneously. This should never happen!"
            )
    except Exception as e:
        logger.error(str(e))
        errors.capture_exception(e)

    update_node_metrics(NODES, destroyed_node_ids)


def update_node_metrics(nodes: Dict[str, Node], destroyed_node_ids: List[str]):
    AVAILABLE_NODES.set(len(nodes))

    for node_id, node in nodes.items():
        SHARD_COUNT.set(node.shard_count, labels=dict(node=node_id))
        LOAD_SCORE.set(node.load_score, labels=dict(node=node_id))

    for node_id in destroyed_node_ids:
        for gauge in (SHARD_COUNT, LOAD_SCORE):
            try:
                gauge.remove(labels=dict(node=node_id))
            except KeyError:
                # Be resilient if there were no previous
                # samples for this node_id
                pass


if __name__ == "__main__":  # pragma: no cover
    # run chitchat server locally without dependencies
    import logging

    logging.basicConfig(level=logging.DEBUG)

    async def run_forever():
        cc = await start_chitchat("test")
        await cc.task

    asyncio.run(run_forever())
