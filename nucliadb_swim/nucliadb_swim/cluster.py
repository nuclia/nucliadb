import asyncio
from asyncio import Queue
from typing import Any, List

from nucliadb_swim.config import ArtilleryClusterConfig
from nucliadb_swim.member import NodeType
from nucliadb_swim.protocol import SocketAddr
from nucliadb_swim.state import (
    ArtilleryClusterEvent,
    ArtilleryClusterRequest,
    ArtilleryClusterRequestTypes,
    ArtilleryEpidemic,
)


def default(obj: Any) -> List[Any]:
    if isinstance(obj, bytes):
        return list(obj)
    raise TypeError


class ArtilleryCluster:
    comm: Queue[ArtilleryClusterRequestTypes]
    loop_task: asyncio.Task

    def __init__(self, comm: Queue[ArtilleryClusterRequestTypes], task: asyncio.Task):
        self.comm = comm
        self.loop_task = task

    @classmethod
    async def create_and_start(
        cls, host_id: str, node_type: NodeType, config: ArtilleryClusterConfig
    ):
        event_queue: Queue[ArtilleryClusterEvent] = Queue()
        internal_queue: Queue[ArtilleryClusterRequestTypes] = Queue()
        state = await ArtilleryEpidemic.new(
            host_id, config, node_type, event_queue, internal_queue
        )
        loop_task = asyncio.create_task(
            ArtilleryEpidemic.event_loop(internal_queue, state)
        )
        return ArtilleryCluster(comm=internal_queue, task=loop_task), event_queue

    def add_seed_node(self, addr: SocketAddr) -> None:
        self.comm.put_nowait(ArtilleryClusterRequest.AddSeed(value=addr))

    def send_payload(self, id: str, msg: Any) -> None:
        self.comm.put_nowait(ArtilleryClusterRequest.Payload(value=[id, str(msg)]))

    def leave_cluster(self) -> None:
        self.comm.put_nowait(ArtilleryClusterRequest.LeaveCluster())

    def exit(self):
        self.comm.put_nowait(ArtilleryClusterRequest.Exit())

    def __del__(self):
        if self.loop_task.done() is False and self.loop_task.cancelled() is False:
            self.loop_task.cancel()
