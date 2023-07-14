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
import asyncio
import concurrent.futures
import logging
import os
from dataclasses import dataclass

import kubernetes
from nucliadb_protos.noderesources_pb2 import EmptyQuery

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.discovery.abc import AbstractClusterDiscovery
from nucliadb.common.cluster.index_node import IndexNode
from nucliadb.common.cluster.settings import Settings
from nucliadb_protos import nodewriter_pb2, nodewriter_pb2_grpc
from nucliadb_utils.grpc import get_traced_grpc_channel

logger = logging.getLogger(__name__)


@dataclass
class NodeData:
    node_id: str
    sts_name: str
    shard_count: int


class KubernetesDiscovery(AbstractClusterDiscovery):
    """
    Load cluster members from kubernetes.
    """

    cluster_task: asyncio.Task
    watch_update_queue_task: asyncio.Task
    update_node_data_cache_task: asyncio.Task
    k8s_watch: kubernetes.watch.Watch

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.queue = asyncio.Queue()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.k8s_watch = kubernetes.watch.Watch()
        self.node_id_cache: dict[str, NodeData] = {}
        self.exit = False

    async def get_node_metadata(self, sts_name: str) -> nodewriter_pb2.NodeMetadata:
        address = f"{sts_name}.node.nucliadb.svc.cluster.local"
        grpc_address = f"{address}:{self.settings.node_writer_port}"
        channel = get_traced_grpc_channel(grpc_address, "discovery", variant="_writer")
        stub = nodewriter_pb2_grpc.NodeWriterStub(channel)
        return await stub.GetMetadata(EmptyQuery())  # type: ignore

    async def get_node_data(self, sts_name: str) -> NodeData:
        if sts_name not in self.node_id_cache:
            metadata = await self.get_node_metadata(sts_name)
            self.node_id_cache[sts_name] = NodeData(
                node_id=metadata.node_id,
                sts_name=sts_name,
                shard_count=metadata.shard_count,
            )
        return self.node_id_cache[sts_name]

    async def update_node(self, event):
        ready = event["object"].status.container_statuses is not None
        for status in event["object"].status.container_statuses or []:
            if status.name not in ("reader", "writer", "sidecar"):
                continue
            if not status.ready:
                ready = False
                break

        sts_name = event["object"].metadata.name
        node_data = await self.get_node_data(sts_name)
        address = f"{sts_name}.node.nucliadb.svc.cluster.local"
        if ready:
            logger.debug(f"Update node {sts_name}: {node_data.node_id}")
            node = manager.get_index_node(node_data.node_id)
            if node is None:
                logger.debug(f"Adding node", extra={"node_id": node_data.node_id})
                manager.add_index_node(
                    IndexNode(
                        id=node_data.node_id,
                        address=address,
                        shard_count=node_data.shard_count,
                    )
                )
            else:
                node.address = address
                node.shard_count = node_data.shard_count
        else:
            logger.debug(f"Remove node", extra={"node_id": node_data.node_id})
            node = manager.get_index_node(node_data.node_id)
            if node is not None:
                manager.remove_index_node(node_data.node_id)

    async def watch_update_queue(self) -> None:
        while True:
            try:
                event = await self.queue.get()
                await self.update_node(event)
            except asyncio.CancelledError:  # pragma: no cover
                return
            except Exception:  # pragma: no cover
                logger.exception("Error while processing cluster event.")
            finally:  # pragma: no cover
                try:
                    self.queue.task_done()
                except ValueError:
                    ...

    def _initialize_cluster(self) -> None:
        if os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token"):
            kubernetes.config.load_incluster_config()
        else:
            kubernetes.config.load_kube_config()
        v1 = kubernetes.client.CoreV1Api()

        while not self.exit:
            for event in self.k8s_watch.stream(
                v1.list_namespaced_pod,
                namespace="nucliadb",
                label_selector="app.kubernetes.io/instance=node",
                timeout_seconds=5,
            ):
                self.queue.put_nowait(event)

    async def update_node_data_cache(self) -> None:
        while True:
            await asyncio.sleep(60)
            try:
                for sts_name in list(self.node_id_cache.keys()):
                    node_data = await self.get_node_data(sts_name)
                    self.node_id_cache[sts_name] = node_data
                    node = manager.get_index_node(node_data.node_id)
                    if node is not None:
                        node.shard_count = node_data.shard_count
            except asyncio.CancelledError:  # pragma: no cover
                return
            except Exception:  # pragma: no cover
                logger.exception("Error while updating shard info.")

    async def initialize(self) -> None:
        self.cluster_task = asyncio.get_event_loop().run_in_executor(
            self.executor, self._initialize_cluster
        )
        self.watch_update_queue_task = asyncio.create_task(self.watch_update_queue())
        self.update_node_data_cache_task = asyncio.create_task(
            self.update_node_data_cache()
        )

    async def finalize(self) -> None:
        self.exit = True
        self.k8s_watch.stop()
        self.cluster_task.cancel()
        self.watch_update_queue_task.cancel()
        self.update_node_data_cache_task.cancel()
