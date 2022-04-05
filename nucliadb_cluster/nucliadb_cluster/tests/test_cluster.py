import asyncio
import logging.config
import os
import uuid
from typing import Any, List, Optional

import pytest

from nucliadb_cluster.cluster import Cluster, Member
from nucliadb_swim.member import NodeType
from nucliadb_swim.protocol import SocketAddr

repo_root_path = os.path.dirname(__file__)
logging.config.fileConfig(
    repo_root_path + "/test_logging.ini", disable_existing_loggers=True
)


@pytest.mark.asyncio
async def test_basic_swim(event_loop: Any) -> None:
    cluster = Cluster(
        "node1",
        SocketAddr("localhost:3133"),
        NodeType.Node,
        ping_interval=2,
        ping_timeout=5,
    )

    cluster2 = Cluster(
        uuid.uuid4().hex,
        SocketAddr("localhost:4144"),
        NodeType.Writer,
        ping_interval=2,
        ping_timeout=5,
        peers=[SocketAddr("localhost:3133")],
    )

    swim_task = asyncio.create_task(cluster.start(), name="SWIM")

    swim2_task = asyncio.create_task(cluster2.start(), name="SWIM2")

    count = 0
    while count < 10:
        try:
            items: Optional[List[Member]] = cluster2.members.get_nowait()
            if items is not None and len(items) > 1:
                assert items[0].is_self
                assert not items[1].is_self
                break
        except asyncio.QueueEmpty:
            items = None
        count += 1
        await asyncio.sleep(1)

    assert count < 10

    count = 0
    while count < 10:
        try:
            items = cluster.members.get_nowait()
            if len(items) > 1:
                assert items[0].is_self
                assert not items[1].is_self
                break
        except asyncio.QueueEmpty:
            items = None
        count += 1
        await asyncio.sleep(1)
    assert count < 10

    with pytest.raises(asyncio.QueueEmpty):
        items = cluster.members.get_nowait()
    with pytest.raises(asyncio.QueueEmpty):
        items = cluster2.members.get_nowait()

    await cluster.leave()
    await cluster.exit()
    swim_task.cancel()

    await cluster2.leave()
    await cluster2.exit()
    swim2_task.cancel()
