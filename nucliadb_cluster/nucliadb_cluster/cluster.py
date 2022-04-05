import asyncio
from dataclasses import dataclass
from typing import List

from nucliadb_cluster import logger
from nucliadb_swim.cluster import ArtilleryCluster
from nucliadb_swim.config import ArtilleryClusterConfig
from nucliadb_swim.member import ArtilleryMember, ArtilleryMemberState, NodeType
from nucliadb_swim.protocol import SocketAddr
from nucliadb_swim.state import (
    ArtilleryClusterEvent,
    ArtilleryMemberEvent,
    ArtilleryMemberEventType,
)

CLUSTER_ID = b"nucliadb-cluster"


@dataclass
class Member:
    node_id: str
    listen_addr: SocketAddr
    node_type: NodeType
    is_self: bool
    online: bool


class Cluster:
    listen_addr: SocketAddr
    artillery_cluster: ArtilleryCluster
    members: asyncio.Queue[List[Member]]
    swim_events_queue: asyncio.Queue[ArtilleryClusterEvent]
    stop: bool
    exited: bool
    node_type: NodeType
    peers: List[SocketAddr] = []

    def __init__(
        self,
        node_id: str,
        listen_addr: SocketAddr,
        node_type: NodeType,
        ping_interval: int = 1,
        ping_timeout: int = 1,
        peers: List[SocketAddr] = [],
    ):
        self.node_id = node_id
        self.node_type = node_type
        self.listen_addr = listen_addr
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.peers = peers
        self.members = asyncio.Queue()
        self.stop = False
        self.exited = False

    async def start(self) -> None:
        self.config = ArtilleryClusterConfig(
            cluster_key=CLUSTER_ID,
            listen_addr=self.listen_addr,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
        )
        (
            self.artillery_cluster,
            self.swim_events_queue,
        ) = await ArtilleryCluster.create_and_start(
            self.node_id, self.node_type, self.config
        )

        self.stop = False

        member = Member(
            node_id=self.node_id,
            listen_addr=self.listen_addr,
            node_type=self.node_type,
            is_self=True,
            online=True,
        )
        self.members.put_nowait([member])

        for peer in self.peers:
            self.add_peer_node(peer)

        await self.event_loop()

    async def event_loop(self) -> None:
        logger.debug("Cluster event loop started")
        while self.stop is False:
            try:
                (
                    artillery_members,
                    artillery_member_event,
                ) = await self.swim_events_queue.get()
            except RuntimeError:
                logger.exception("Stop receiving messages")
                break
            self.log_artillery_event(artillery_member_event)
            updated_memberlist = [
                convert_member(x, self.listen_addr)
                for x in artillery_members
                if x.state()
                in (ArtilleryMemberState.Alive, ArtilleryMemberState.Suspect)
            ]
            self.members.put_nowait(updated_memberlist)
        self.exited = True

    def add_peer_node(self, peer_addr: SocketAddr) -> None:
        self.artillery_cluster.add_seed_node(peer_addr)

    async def leave(self) -> None:
        self.artillery_cluster.leave_cluster()
        self.stop = True
        while self.exited is False:
            await asyncio.sleep(0.5)

    async def exit(self):
        self.artillery_cluster.exit()
        while (
            self.artillery_cluster.loop_task.done() is False
            and self.artillery_cluster.loop_task.cancelled() is False
        ):
            await asyncio.sleep(0.5)

        # del self.artillery_cluster

    def log_artillery_event(
        self, artillery_member_event: ArtilleryMemberEventType
    ) -> None:
        if isinstance(artillery_member_event, ArtilleryMemberEvent.Joined):
            joined_member = artillery_member_event.value
            logger.info(
                f"{self.node_id}/{self.node_type}: \
                  {joined_member.node_id}/{joined_member.node_type} \
                  {joined_member.remote_host} Joined"
            )
        elif isinstance(artillery_member_event, ArtilleryMemberEvent.WentUp):
            wentup_member = artillery_member_event.value
            logger.info(
                f"{self.node_id}/{self.node_type}: \
                  {wentup_member.node_id}/{wentup_member.node_type} \
                  {wentup_member.remote_host} Went up"
            )
        elif isinstance(artillery_member_event, ArtilleryMemberEvent.SuspectedDown):
            suspect_down = artillery_member_event.value
            logger.info(
                f"{self.node_id}/{self.node_type}: \
                  {suspect_down.node_id}/{suspect_down.node_type} \
                  {suspect_down.remote_host} Suspected down"
            )
        elif isinstance(artillery_member_event, ArtilleryMemberEvent.WentDown):
            down_member = artillery_member_event.value
            logger.info(
                f"{self.node_id}/{self.node_type}: \
                  {down_member.node_id}/{down_member.node_type} \
                  {down_member.remote_host} Went down"
            )
        elif isinstance(artillery_member_event, ArtilleryMemberEvent.Left):
            left_member = artillery_member_event.value
            logger.info(
                f"{self.node_id}/{self.node_type}: \
                  {left_member.node_id}/{left_member.node_type} \
                  {left_member.remote_host} Left"
            )
        elif isinstance(artillery_member_event, ArtilleryMemberEvent.Payload):
            payload_event = artillery_member_event.value
            logger.info(
                f"{self.node_id}/{self.node_type}: \
                  {payload_event[0].node_id}/{payload_event[0].node_type} \
                  {payload_event[0].remote_host} {payload_event[1]} Payload"
            )


def convert_member(member: ArtilleryMember, self_listen_addr: SocketAddr) -> Member:
    addr = member.remote_host if member.remote_host else self_listen_addr
    return Member(
        node_id=member.node_id,
        listen_addr=addr,
        node_type=member.node_type,
        is_self=member.is_current(),
        online=member.state() == ArtilleryMemberState.Alive,
    )
