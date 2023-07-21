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
from typing import TypedDict

import kubernetes_asyncio.client  # type: ignore
import kubernetes_asyncio.client.models.v1_container_status  # type: ignore
import kubernetes_asyncio.client.models.v1_object_meta  # type: ignore
import kubernetes_asyncio.client.models.v1_pod  # type: ignore
import kubernetes_asyncio.client.models.v1_pod_status  # type: ignore
import kubernetes_asyncio.config  # type: ignore
import kubernetes_asyncio.watch  # type: ignore
from nucliadb_protos.noderesources_pb2 import EmptyQuery

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.discovery.abc import (
    AVAILABLE_NODES,
    AbstractClusterDiscovery,
)
from nucliadb.common.cluster.discovery.types import IndexNodeMetadata
from nucliadb.common.cluster.index_node import IndexNode
from nucliadb.common.cluster.settings import Settings
from nucliadb_protos import nodewriter_pb2, nodewriter_pb2_grpc
from nucliadb_utils.grpc import get_traced_grpc_channel

logger = logging.getLogger(__name__)


class EventType(TypedDict):
    type: str
    object: kubernetes_asyncio.client.models.v1_pod.V1Pod


class KubernetesDiscovery(AbstractClusterDiscovery):
    """
    Load cluster members from kubernetes.
    """

    cache_update_interval = 60
    cluster_task: asyncio.Task
    update_node_data_cache_task: asyncio.Task

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.node_id_cache: dict[str, IndexNodeMetadata] = {}
        self.update_lock = asyncio.Lock()

    async def _query_node_metadata(self, address: str) -> nodewriter_pb2.NodeMetadata:
        """
        Get node metadata directly from the writer.

        Establishes a new connection on every try on purpose to avoid long lived connections
        and dns caching issues.

        This method should be used carefully and results should be cached.
        """
        grpc_address = f"{address}:{self.settings.node_writer_port}"
        channel = get_traced_grpc_channel(grpc_address, "discovery", variant="_writer")
        stub = nodewriter_pb2_grpc.NodeWriterStub(channel)
        return await stub.GetMetadata(EmptyQuery())  # type: ignore

    async def get_node_metadata(self, sts_name: str, node_ip: str) -> IndexNodeMetadata:
        async with self.update_lock:
            if sts_name not in self.node_id_cache:
                # hard coded assuming that nucliadb is used with current helm charts
                # XXX right now, GRPC will not know when an IP has changed and
                # istio is not able to handle this properly. This is a workaround for now.
                # address = f"{sts_name}.node.{self.settings.cluster_discovery_kubernetes_namespace}.svc.cluster.local"
                metadata = await self._query_node_metadata(node_ip)
                self.node_id_cache[sts_name] = IndexNodeMetadata(
                    node_id=metadata.node_id,
                    name=sts_name,
                    shard_count=metadata.shard_count,
                    address=node_ip,
                )
            else:
                self.node_id_cache[sts_name].address = node_ip
        return self.node_id_cache[sts_name]

    async def update_node(self, event: EventType) -> None:
        """
        Update node metadata when a pod is updated.

        This method will update global node state by utilizing the cluster manager
        to add or remove nodes.
        """
        status: kubernetes_asyncio.client.models.v1_pod_status.V1PodStatus = event[
            "object"
        ].status
        event_metadata: kubernetes_asyncio.client.models.v1_object_meta.V1ObjectMeta = (
            event["object"].metadata
        )

        ready = status.container_statuses is not None
        if event["type"] == "DELETED":
            ready = False
        elif status.container_statuses is not None:
            container_statuses: list[
                kubernetes_asyncio.client.models.v1_container_status.V1ContainerStatus
            ] = status.container_statuses
            for container_status in container_statuses:
                if container_status.name not in ("reader", "writer", "sidecar"):
                    continue
                if not container_status.ready:
                    ready = False
                    break

        sts_name = event_metadata.name
        node_data = await self.get_node_metadata(sts_name, status.pod_ip)
        if ready:
            node = manager.get_index_node(node_data.node_id)
            if node is None:
                logger.warning(f"Adding node", extra={"node_id": node_data.node_id})
                manager.add_index_node(
                    IndexNode(
                        id=node_data.node_id,
                        address=node_data.address,
                        shard_count=node_data.shard_count,
                    )
                )
            else:
                logger.debug(
                    "Update node",
                    extra={"sts_name": sts_name, "node_id": node_data.node_id},
                )
                node.address = node_data.address
                node.shard_count = node_data.shard_count
        else:
            node = manager.get_index_node(node_data.node_id)
            if node is not None:
                logger.warning(f"Remove node", extra={"node_id": node_data.node_id})
                manager.remove_index_node(node_data.node_id)

        AVAILABLE_NODES.set(len(manager.get_index_nodes()))

    async def watch_k8s_for_updates(self) -> None:
        if os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token"):
            kubernetes_asyncio.config.load_incluster_config()
        else:
            await kubernetes_asyncio.config.load_kube_config()
        v1 = kubernetes_asyncio.client.CoreV1Api()

        watch = kubernetes_asyncio.watch.Watch()
        try:
            while True:
                try:
                    async for event in watch.stream(
                        v1.list_namespaced_pod,
                        namespace="nucliadb",
                        label_selector="app.kubernetes.io/instance=node",
                        timeout_seconds=30,
                    ):
                        await self.update_node(event)
                except (
                    asyncio.CancelledError,
                    KeyboardInterrupt,
                    SystemExit,
                    RuntimeError,
                ):  # pragma: no cover
                    return
                except Exception:  # pragma: no cover
                    logger.exception(
                        "Error while watching kubernetes. Trying again in 5 seconds.",
                        exc_info=True,
                    )
                    await asyncio.sleep(5)
        finally:
            watch.stop()
            await watch.close()

    async def update_node_data_cache(self) -> None:
        while True:
            await asyncio.sleep(self.cache_update_interval)
            try:
                for sts_name in list(self.node_id_cache.keys()):
                    # force updating cache
                    async with self.update_lock:
                        existing = self.node_id_cache[sts_name]
                        metadata = await self._query_node_metadata(existing.address)
                        self.node_id_cache[sts_name] = IndexNodeMetadata(
                            node_id=metadata.node_id,
                            name=sts_name,
                            shard_count=metadata.shard_count,
                            address=existing.address,
                        )
            except (
                asyncio.CancelledError,
                KeyboardInterrupt,
                SystemExit,
                RuntimeError,
            ):  # pragma: no cover
                return
            except Exception:  # pragma: no cover
                logger.exception("Error while updating shard info.")

    async def initialize(self) -> None:
        self.cluster_task = asyncio.create_task(self.watch_k8s_for_updates())
        self.update_node_data_cache_task = asyncio.create_task(
            self.update_node_data_cache()
        )

    async def finalize(self) -> None:
        self.cluster_task.cancel()
        self.update_node_data_cache_task.cancel()
