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
import time
from typing import TypedDict

import kubernetes_asyncio.client  # type: ignore
import kubernetes_asyncio.client.models.v1_container_status  # type: ignore
import kubernetes_asyncio.client.models.v1_object_meta  # type: ignore
import kubernetes_asyncio.client.models.v1_pod  # type: ignore
import kubernetes_asyncio.client.models.v1_pod_status  # type: ignore
import kubernetes_asyncio.config  # type: ignore
import kubernetes_asyncio.watch  # type: ignore

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.discovery.base import (
    AVAILABLE_NODES,
    AbstractClusterDiscovery,
)
from nucliadb.common.cluster.discovery.types import IndexNodeMetadata
from nucliadb.common.cluster.exceptions import NodeConnectionError
from nucliadb.common.cluster.settings import Settings

logger = logging.getLogger(__name__)


class EventType(TypedDict):
    type: str
    object: kubernetes_asyncio.client.models.v1_pod.V1Pod


class KubernetesDiscovery(AbstractClusterDiscovery):
    """
    Load cluster members from kubernetes.
    """

    node_heartbeat_interval = 10
    cluster_task: asyncio.Task
    update_node_data_cache_task: asyncio.Task

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.node_id_cache: dict[str, IndexNodeMetadata] = {}
        self.update_lock = asyncio.Lock()

    async def get_node_metadata(
        self, pod_name: str, node_ip: str, read_replica: bool
    ) -> IndexNodeMetadata:
        async with self.update_lock:
            if pod_name not in self.node_id_cache:
                self.node_id_cache[pod_name] = await self._query_node_metadata(node_ip, read_replica)
            else:
                self.node_id_cache[pod_name].address = node_ip
            self.node_id_cache[pod_name].updated_at = time.time()
        return self.node_id_cache[pod_name]

    async def update_node(self, event: EventType) -> None:
        """
        Update node metadata when a pod is updated.

        This method will update global node state by utilizing the cluster manager
        to add or remove nodes.
        """
        status: kubernetes_asyncio.client.models.v1_pod_status.V1PodStatus = event["object"].status
        event_metadata: kubernetes_asyncio.client.models.v1_object_meta.V1ObjectMeta = event[
            "object"
        ].metadata

        ready = status.container_statuses is not None
        if event["type"] == "DELETED":
            ready = False
        elif status.container_statuses is not None:
            container_statuses: list[
                kubernetes_asyncio.client.models.v1_container_status.V1ContainerStatus
            ] = status.container_statuses
            for container_status in container_statuses:
                if container_status.name not in ("reader", "writer"):
                    continue
                if not container_status.ready or status.pod_ip is None:
                    ready = False
                    break

        pod_name = event_metadata.name
        read_replica = event_metadata.labels.get("readReplica", "") == "true"
        if not ready:
            if pod_name not in self.node_id_cache:
                logger.debug(
                    "Node not ready and not in cache, ignore",
                    extra={"pod_name": pod_name},
                )
                return
            else:
                node_data = self.node_id_cache[pod_name]
        else:
            try:
                node_data = await self.get_node_metadata(
                    pod_name,
                    status.pod_ip,
                    read_replica=read_replica,
                )
            except NodeConnectionError:  # pragma: no cover
                logger.warning(
                    "Error connecting to node",
                    extra={
                        "pod_name": pod_name,
                        "node_ip": status.pod_ip,
                        "read_replica": read_replica,
                    },
                )
                raise

        if ready:
            node = manager.get_index_node(node_data.node_id)
            if node is None:
                logger.info(
                    "Adding node",
                    extra={
                        "node_id": node_data.node_id,
                        "pod_name": pod_name,
                        "address": node_data.address,
                    },
                )
                manager.add_index_node(
                    id=node_data.node_id,
                    address=node_data.address,
                    shard_count=node_data.shard_count,
                    available_disk=node_data.available_disk,
                    primary_id=node_data.primary_id,
                )
            else:
                logger.debug(
                    "Update node",
                    extra={"pod_name": pod_name, "node_id": node_data.node_id},
                )
                node.address = node_data.address
                node.shard_count = node_data.shard_count
        else:
            node = manager.get_index_node(node_data.node_id)
            if node is not None:
                logger.info(
                    f"Remove node",
                    extra={
                        "node_id": node_data.node_id,
                        "pod_name": pod_name,
                        "address": node.address,
                    },
                )
                manager.remove_index_node(node_data.node_id, node_data.primary_id)
                if pod_name in self.node_id_cache:
                    del self.node_id_cache[pod_name]

        AVAILABLE_NODES.set(len(manager.get_index_nodes()))

    async def watch_k8s_for_updates(self) -> None:
        if os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token"):
            kubernetes_asyncio.config.load_incluster_config()
        else:
            await kubernetes_asyncio.config.load_kube_config()

        async with kubernetes_asyncio.client.ApiClient() as api:
            v1 = kubernetes_asyncio.client.CoreV1Api(api)
            watch = kubernetes_asyncio.watch.Watch()
            try:
                while True:
                    try:
                        async for event in watch.stream(
                            v1.list_namespaced_pod,
                            namespace=self.settings.cluster_discovery_kubernetes_namespace,
                            label_selector=self.settings.cluster_discovery_kubernetes_selector,
                            timeout_seconds=30,
                        ):
                            try:
                                await self.update_node(event)
                            except NodeConnectionError:  # pragma: no cover
                                pass
                            except Exception:  # pragma: no cover
                                logger.exception("Error while updating node", exc_info=True)
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

    def _maybe_remove_stale_node(self, pod_name: str) -> None:
        """
        This is rare but possible to reproduce under contrived API usage scenarios to
        get in a situation where we do not remove a node from a cluster because we missed
        a removal event.

        It seems to be possible that we miss events from kubernetes.

        We should view getting node metadata as a health check just in case.
        """
        if pod_name not in self.node_id_cache:
            return

        node_data = self.node_id_cache[pod_name]
        if time.time() - node_data.updated_at > (self.node_heartbeat_interval * 2):
            node = manager.get_index_node(node_data.node_id)
            if node is not None:
                logger.warning(
                    f"Removing stale node {pod_name} {node_data.address}",
                    extra={
                        "node_id": node_data.node_id,
                        "pod_name": pod_name,
                        "address": node_data.address,
                    },
                )
                manager.remove_index_node(node_data.node_id, node_data.primary_id)
            del self.node_id_cache[pod_name]

    async def update_node_data_cache(self) -> None:
        while True:
            await asyncio.sleep(self.node_heartbeat_interval)
            try:
                for pod_name in list(self.node_id_cache.keys()):
                    # force updating cache
                    async with self.update_lock:
                        if pod_name not in self.node_id_cache:
                            # could change in the meantime since we're waiting for lock
                            continue
                        existing = self.node_id_cache[pod_name]
                        try:
                            self.node_id_cache[pod_name] = await self._query_node_metadata(
                                existing.address,
                                read_replica=existing.primary_id is not None,
                            )
                        except NodeConnectionError:  # pragma: no cover
                            self._maybe_remove_stale_node(pod_name)
            except (
                asyncio.CancelledError,
                KeyboardInterrupt,
                SystemExit,
                RuntimeError,
            ):  # pragma: no cover
                return
            except Exception:  # pragma: no cover
                logger.exception("Error while updating shard info.")

    async def _wait_ready(self, max_wait: int = 60) -> None:
        """
        Attempt to wait for the cluster to be ready.
        Since we don't know the number of nodes that the cluster will have, we assume
        that the cluster is ready when the number of nodes is stable for 3 consecutive checks.
        """
        ready = False
        success = 0
        start = time.monotonic()
        logger.info("Waiting for cluster to be ready.")
        while time.monotonic() - start < max_wait:
            await asyncio.sleep(0.25)
            if len(manager.get_index_nodes()) > 0:
                success += 1
            else:
                success = 0
            if success >= 3:
                ready = True
                break
        if not ready:
            logger.warning(f"Cluster not ready after {max_wait} seconds.")

    async def initialize(self) -> None:
        self.cluster_task = asyncio.create_task(self.watch_k8s_for_updates())
        self.update_node_data_cache_task = asyncio.create_task(self.update_node_data_cache())
        await self._wait_ready()

    async def finalize(self) -> None:
        self.cluster_task.cancel()
        self.update_node_data_cache_task.cancel()
