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
from nucliadb_protos.noderesources_pb2 import EmptyQuery
import kubernetes
from nucliadb_models.cluster import ClusterMember
from nucliadb_protos import nodewriter_pb2, nodewriter_pb2_grpc
from nucliadb_utils.grpc import get_traced_grpc_channel
import os
from .abc import AbstractClusterDiscovery, update_members
from ..settings import Settings
import concurrent.futures
import logging

logger = logging.getLogger(__name__)


class KubernetesDiscovery(AbstractClusterDiscovery):
    """
    Load cluster members from kubernetes.
    """

    cluster_task: asyncio.Task
    watch_task: asyncio.Task

    def __init__(self, settings: Settings) -> None:
        super().__init__(settings)
        self.queue = asyncio.Queue()
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    async def discover(self) -> None:
        members = []
        for index in range(self.settings.cluster_discovery_k8s_number_of_nodes):
            address = f"node-{index}.node.nucliadb.svc.cluster.local"
            grpc_address = f"{address}:{self.settings.node_writer_port}"
            channel = get_traced_grpc_channel(
                grpc_address, "discovery", variant="_writer"
            )
            stub = nodewriter_pb2_grpc.NodeWriterStub(channel)
            metadata: nodewriter_pb2.NodeMetadata = await stub.GetMetadata(EmptyQuery())  # type: ignore
            members.append(
                ClusterMember(
                    id=metadata.node_id,
                    address=address,
                    shard_count=metadata.shard_count,
                )
            )
        update_members(members)

    async def watch(self) -> None:
        while True:
            try:
                event = await self.queue.get()
                print(f'Event: {event["type"]} - for {event["object"].metadata.name}')
                # if event["type"] == "ADDED":
                #     logger.info(f"Added node {event['object'].metadata.name}")
                # elif event["type"] == "DELETED":
                #     logger.info(f"Deleted node {event['object'].metadata.name}")
                # elif event["type"] == "MODIFIED":
                #     logger.info(f"Modified node {event['object'].metadata.name}")
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("Error while processing cluster event.")
            finally:
                self.queue.task_done()

    def _initialize_cluster(self) -> None:
        if os.path.exists("/var/run/secrets/kubernetes.io/serviceaccount/token"):
            kubernetes.config.load_incluster_config()
        else:
            kubernetes.config.load_kube_config()
        v1 = kubernetes.client.CoreV1Api()
        w = kubernetes.watch.Watch()

        while True:
            for event in w.stream(
                v1.list_namespaced_pod,
                namespace="nucliadb",
                label_selector="app.kubernetes.io/instance=node",
            ):
                self.queue.put_nowait(event)

    async def initialize(self) -> None:
        self.cluster_task = asyncio.get_event_loop().run_in_executor(
            self.executor, self._initialize_cluster
        )
        self.watch_task = asyncio.create_task(self.watch())

    async def finalize(self) -> None:
        self.cluster_task.cancel()
        self.watch_task.cancel()
