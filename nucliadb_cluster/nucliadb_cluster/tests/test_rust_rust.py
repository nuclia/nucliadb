import asyncio
import logging.config
import os
from typing import Any, List, Optional

import pytest
from nucliadb_cluster_rust import Cluster as ClusterRust

from nucliadb_cluster.cluster import Member
from nucliadb_swim.member import NodeType

repo_root_path = os.path.dirname(__file__)
logging.config.fileConfig(
    repo_root_path + "/test_logging.ini", disable_existing_loggers=True
)


@pytest.mark.asyncio
async def test_rust_rust_swim(event_loop: Any) -> None:
    cluster = ClusterRust("node1", "127.0.0.1:3533", NodeType.Node.value, 5, 1)

    cluster2 = ClusterRust("writer1", "127.0.0.1:4544", NodeType.Writer.value, 5, 1)
    cluster2.add_peer_node("127.0.0.1:3533")

    cluster3 = ClusterRust("reader1", "127.0.0.1:3532", NodeType.Reader.value, 5, 1)
    cluster3.add_peer_node("127.0.0.1:3533")

    cluster4 = ClusterRust("node2", "127.0.0.1:4542", NodeType.Node.value, 5, 1)
    cluster4.add_peer_node("127.0.0.1:3533")

    count = 0
    while count < 10:
        try:
            items: Optional[List[Member]] = cluster2.get_members()
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
            items = cluster.get_members()
            if len(items) > 3:
                break
        except asyncio.QueueEmpty:
            items = None
        count += 1
        await asyncio.sleep(1)
    assert count < 10

    count = 0
    while count < 10:
        items = cluster3.get_members()
        if len(items) > 3:
            break
        count += 1
        await asyncio.sleep(1)
    assert count < 10

    count = 0
    while count < 10:
        try:
            items = cluster4.get_members()
            if len(items) > 3:
                break
        except asyncio.QueueEmpty:
            items = None
        count += 1
        await asyncio.sleep(1)
    assert count < 10
