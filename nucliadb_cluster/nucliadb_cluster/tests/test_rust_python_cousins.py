import asyncio
import logging.config
import os
from typing import Any, List, Optional

import pytest
from nucliadb_cluster_rust import Cluster as ClusterRust

from nucliadb_cluster.cluster import Cluster, Member
from nucliadb_swim.member import NodeType
from nucliadb_swim.protocol import SocketAddr

repo_root_path = os.path.dirname(__file__)
logging.config.fileConfig(
    repo_root_path + "/test_logging.ini", disable_existing_loggers=True
)


@pytest.mark.asyncio
async def test_rust_python_swim_cousins(event_loop: Any) -> None:
    cluster3 = ClusterRust("rust0", "127.0.0.1:3442", NodeType.Reader.value, 10, 3)

    cluster = Cluster(
        "python0",
        SocketAddr("127.0.0.1:3443"),
        NodeType.Node,
        ping_interval=3,
        ping_timeout=10,
        peers=[SocketAddr("127.0.0.1:3442")],
    )

    cluster2 = Cluster(
        "python1",
        SocketAddr("127.0.0.1:4554"),
        NodeType.Writer,
        ping_interval=3,
        ping_timeout=10,
        peers=[SocketAddr("127.0.0.1:3442")],
    )

    cluster4 = ClusterRust("rust1", "127.0.0.1:4552", NodeType.Node.value, 10, 3)
    cluster4.add_peer_node("127.0.0.1:3442")

    cluster5 = ClusterRust("rust2", "127.0.0.1:5662", NodeType.Node.value, 10, 3)
    cluster5.add_peer_node("127.0.0.1:3442")

    swim_task = asyncio.create_task(cluster.start(), name="SWIM")
    swim2_task = asyncio.create_task(cluster2.start(), name="SWIM2")

    count = 0
    while count < 10:
        try:
            items: Optional[List[Member]] = cluster2.members.get_nowait()
            if items is not None and len(items) > 3:
                break
        except asyncio.QueueEmpty:
            items = None
        count += 1
        await asyncio.sleep(1)
        await asyncio.sleep(1)

    assert count < 10

    count = 0
    while count < 10:
        try:
            items = cluster.members.get_nowait()
            if len(items) > 4:
                break
        except asyncio.QueueEmpty:
            items = None
        count += 1
        await asyncio.sleep(1)
    assert count < 10

    count = 0
    while count < 10:
        items = cluster3.get_members()
        if len(items) > 4:
            break
        count += 1
        await asyncio.sleep(1)
    assert count < 10

    count = 0
    while count < 10:
        try:
            items = cluster4.get_members()
            if len(items) > 4:
                break
        except asyncio.QueueEmpty:
            items = None
        count += 1
        await asyncio.sleep(1)
    assert count < 10

    await cluster.leave()
    await cluster.exit()
    await cluster2.leave()
    await cluster2.exit()

    swim_task.cancel()
    swim2_task.cancel()
