import asyncio
import logging.config
import os
from typing import Any

import pytest

from nucliadb_cluster.cluster import Cluster
from nucliadb_swim.member import NodeType
from nucliadb_swim.protocol import SocketAddr

repo_root_path = os.path.dirname(__file__)
logging.config.fileConfig(
    repo_root_path + "/test_logging.ini", disable_existing_loggers=True
)


@pytest.mark.asyncio
async def not_test_restart_swim(event_loop: Any) -> None:

    node1 = Cluster(
        "node1",
        SocketAddr("127.0.0.1:4541"),
        NodeType.Node,
        ping_interval=5,
        ping_timeout=5,
    )
    node2 = Cluster(
        "node2",
        SocketAddr("127.0.0.1:4542"),
        NodeType.Node,
        ping_interval=5,
        ping_timeout=5,
        peers=[SocketAddr("127.0.0.1:4541")],
    )
    node3 = Cluster(
        "node3",
        SocketAddr("127.0.0.1:4543"),
        NodeType.Node,
        ping_interval=5,
        ping_timeout=5,
        peers=[SocketAddr("127.0.0.1:4541")],
    )

    search = Cluster(
        "search1",
        SocketAddr("127.0.0.1:4545"),
        NodeType.Search,
        ping_interval=5,
        ping_timeout=5,
        peers=[SocketAddr("127.0.0.1:4541")],
    )

    writer = Cluster(
        "write1",
        SocketAddr("127.0.0.1:4544"),
        NodeType.Writer,
        ping_interval=5,
        ping_timeout=5,
        peers=[SocketAddr("127.0.0.1:4541")],
    )

    node1_task = asyncio.create_task(node1.start(), name="NODE1")
    node2_task = asyncio.create_task(node2.start(), name="NODE2")
    node3_task = asyncio.create_task(node3.start(), name="NODE3")

    search_task = asyncio.create_task(search.start(), name="SEARCH")
    writer_task = asyncio.create_task(writer.start(), name="WRITER")

    count = 0
    while count < 20:
        members = await search.members.get()
        if len(members) == 5:
            break
        await asyncio.sleep(1)
        count += 1

    node2.artillery_cluster.loop_task.cancel()
    # await node2.exit()
    node2_task.cancel()

    count = 0
    while count < 20:
        members = await search.members.get()
        if len(members) == 4:
            break
        await asyncio.sleep(1)
        count += 1

    node2 = Cluster(
        "node2",
        SocketAddr("127.0.0.1:4551"),
        NodeType.Node,
        ping_interval=5,
        ping_timeout=5,
        peers=[SocketAddr("127.0.0.1:4541")],
    )
    node2_task = asyncio.create_task(node2.start(), name="NODE2")

    count = 0
    while count < 20:
        members = await search.members.get()
        if len(members) == 5:
            break
        await asyncio.sleep(1)
        count += 1

    await node2.exit()
    node2_task.cancel()

    count = 0
    while count < 20:
        members = await search.members.get()
        if len(members) == 4:
            break
        await asyncio.sleep(1)
        count += 1

    node2 = Cluster(
        "node2",
        SocketAddr("127.0.0.1:4551"),
        NodeType.Node,
        ping_interval=5,
        ping_timeout=5,
        peers=[SocketAddr("127.0.0.1:4541")],
    )
    node2_task = asyncio.create_task(node2.start(), name="NODE2")

    count = 0
    while count < 20:
        members = await search.members.get()
        if len(members) == 5:
            break
        await asyncio.sleep(1)
        count += 1

    await node1.leave()
    await node1.exit()
    node1_task.cancel()

    await node2.leave()
    await node2.exit()
    node2_task.cancel()

    await node3.leave()
    await node3.exit()
    node3_task.cancel()

    await search.leave()
    await search.exit()
    search_task.cancel()

    await writer.leave()
    await writer.exit()
    writer_task.cancel()
