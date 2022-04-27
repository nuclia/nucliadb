from __future__ import annotations

import asyncio
import traceback
from asyncio import BaseTransport, Queue
from copy import deepcopy
from dataclasses import dataclass
from time import time
from typing import Any, Dict, List, Optional, Tuple, Union

import orjson
from pydantic import BaseModel, root_validator

from nucliadb_swim import logger
from nucliadb_swim.config import ArtilleryClusterConfig
from nucliadb_swim.member import (
    ArtilleryMember,
    ArtilleryMemberState,
    ArtilleryStateChange,
    NodeType,
)
from nucliadb_swim.membership import ArtilleryMemberList
from nucliadb_swim.protocol import SocketAddr, SwimProtocol


class ArtilleryMemberEvent:
    class BasicArtilleryMemberEvent(BaseModel):
        value: Any

    class Joined(BasicArtilleryMemberEvent):
        value: ArtilleryMember

    class WentUp(BasicArtilleryMemberEvent):
        value: ArtilleryMember

    class SuspectedDown(BasicArtilleryMemberEvent):
        value: ArtilleryMember

    class WentDown(BasicArtilleryMemberEvent):
        value: ArtilleryMember

    class Left(BasicArtilleryMemberEvent):
        value: ArtilleryMember

    class Payload(BasicArtilleryMemberEvent):
        value: Tuple[ArtilleryMember, str]


ArtilleryMemberEventType = Union[
    ArtilleryMemberEvent.Joined,
    ArtilleryMemberEvent.WentUp,
    ArtilleryMemberEvent.SuspectedDown,
    ArtilleryMemberEvent.WentDown,
    ArtilleryMemberEvent.Left,
    ArtilleryMemberEvent.Payload,
]

ArtilleryClusterEvent = Tuple[List[ArtilleryMember], ArtilleryMemberEventType]


class Request:
    class BasicRequest(BaseModel):
        def is_heartbeat(self):
            return False

    class Heartbeat(BasicRequest):
        Heartbeat: Optional[str] = None

        def is_heartbeat(self):
            return True

    class Ack(BasicRequest):
        Ack: str

    class Ping(BasicRequest):
        Ping: Tuple[SocketAddr, str]

    class AckHost(BasicRequest):
        AckHost: ArtilleryMember

    class Payload(BasicRequest):
        Payload: Tuple[str, str]


RequestTypes = Union[
    Request.Heartbeat, Request.Ack, Request.Ping, Request.AckHost, Request.Payload
]


class ArtilleryMessage(BaseModel):
    node_id: str
    cluster_key: bytes
    node_type: NodeType
    request: Optional[RequestTypes] = None
    state_changes: List[ArtilleryStateChange] = []

    @classmethod
    def parse_message(cls, message: bytes):
        parsed = orjson.loads(message)
        obj = ArtilleryMessage.parse_raw(message)
        if "Heartbeat" in parsed["request"]:
            obj.request = Request.Heartbeat.parse_obj(parsed["request"])

        if "Ack" in parsed["request"]:
            obj.request = Request.Ack.parse_obj(parsed["request"])

        if "Ping" in parsed["request"]:
            obj.request = Request.Ping.parse_obj(parsed["request"])

        if "AckHost" in parsed["request"]:
            obj.request = Request.AckHost.parse_obj(parsed["request"])

        if "Payload" in parsed["request"]:
            obj.request = Request.Payload.parse_obj(parsed["request"])
        return obj

    @root_validator(pre=True)
    def global_validator(cls, v):
        if v is not None and "cluster_key" in v and isinstance(v["cluster_key"], list):
            v["cluster_key"] = bytes(v["cluster_key"])

        return v

    class Config:
        json_encoders = {
            bytes: lambda v: list(v),
            SocketAddr: lambda v: f"{v['host']:v['port']}",
        }
        smart_union = True


class TargetedRequest(BaseModel):
    request: Optional[RequestTypes] = None
    target: SocketAddr
    seed: bool = False


class ArtilleryClusterRequest:
    class BasicRequest(BaseModel):
        pass

    class AddSeed(BasicRequest):
        value: SocketAddr

    class Respond(BasicRequest):
        value: Tuple[SocketAddr, ArtilleryMessage]

    class React(BasicRequest):
        value: TargetedRequest

    class LeaveCluster(BasicRequest):
        pass

    class Exit(BasicRequest):
        pass

    class Payload(BasicRequest):
        value: Tuple[str, str]


ArtilleryClusterRequestTypes = Union[
    ArtilleryClusterRequest.AddSeed,
    ArtilleryClusterRequest.Respond,
    ArtilleryClusterRequest.React,
    ArtilleryClusterRequest.LeaveCluster,
    ArtilleryClusterRequest.Exit,
    ArtilleryClusterRequest.Payload,
]


def default(obj: Any) -> List[Any]:
    if isinstance(obj, bytes):
        return list(obj)
    raise TypeError


@dataclass
class ArtilleryEpidemic:
    host_id: str
    config: ArtilleryClusterConfig
    node_type: NodeType
    members: ArtilleryMemberList
    seed_queue: List[SocketAddr]
    pending_responses: List[Tuple[float, SocketAddr, List[ArtilleryStateChange]]]
    state_changes: List[ArtilleryStateChange]
    wait_list: Dict[SocketAddr, List[SocketAddr]]
    protocol: SwimProtocol
    transport: BaseTransport
    request_queue: Queue[ArtilleryClusterRequestTypes]
    event_queue: Queue[ArtilleryClusterEvent]
    running: bool
    exit: bool = False

    @classmethod
    async def new(
        cls,
        host_id: str,
        config: ArtilleryClusterConfig,
        node_type: NodeType,
        event_queue: Queue[ArtilleryClusterEvent],
        internal_queue: Queue[ArtilleryClusterRequestTypes],
    ) -> ArtilleryEpidemic:
        loop = asyncio.get_event_loop()

        protocol: SwimProtocol
        transport, protocol = await loop.create_datagram_endpoint(  # type: ignore
            SwimProtocol,
            local_addr=(config.listen_addr.host, config.listen_addr.port),
        )
        member = ArtilleryMember.current(host_id, node_type)
        return ArtilleryEpidemic(
            host_id=host_id,
            config=config,
            node_type=node_type,
            members=ArtilleryMemberList(member),
            seed_queue=[],
            pending_responses=[],
            state_changes=[ArtilleryStateChange(member=member)],
            wait_list=dict(),
            protocol=protocol,
            request_queue=internal_queue,
            event_queue=event_queue,
            running=True,
            transport=transport,
        )

    @classmethod
    async def event_loop(
        cls,
        receiver: Queue[ArtilleryClusterRequestTypes],
        state: ArtilleryEpidemic,
    ):
        start = time()
        timeout = state.config.ping_interval
        logger.info("Starting event loop")

        while True:

            elapsed = time() - start
            if elapsed >= timeout:
                state.enqueue_seed_nodes()
                state.enqueue_random_ping()
                start = time()

            if state.running is False:
                break

            # We check all messages on the queue before starting to respond
            pending_message: List[Tuple[bytes, SocketAddr]] = []
            last_pending: Optional[Tuple[bytes, SocketAddr]] = None
            checked = False
            while checked is False and last_pending is None:
                try:
                    last_pending = state.protocol.recv()
                    if last_pending is not None:
                        pending_message.append(last_pending)
                except asyncio.queues.QueueEmpty:
                    last_pending = None
                checked = True

            try:
                while True:
                    msg: Optional[ArtilleryClusterRequestTypes] = receiver.get_nowait()
                    try:
                        if msg is not None:
                            await state.process_internal_request(msg)
                    except Exception:
                        traceback.print_exc()
            except asyncio.queues.QueueEmpty:
                msg = None

            for pending in pending_message:
                message: bytes = pending[0]
                source_address: SocketAddr = pending[1]
                try:
                    art_message = ArtilleryMessage.parse_message(message)
                    await state.request_queue.put(
                        ArtilleryClusterRequest.Respond(
                            value=(source_address, art_message)
                        )
                    )
                except Exception:
                    logger.exception(f"Error parsing message {message.decode()}")
            await asyncio.sleep(0.5)

        socket = state.transport.get_extra_info("socket")
        state.transport.close()
        try:
            socket.close()
        except TypeError:
            # UVLoop fails
            pass
        state.exit = True

    async def process_request(self, request: TargetedRequest) -> None:

        timeout = time() + self.config.ping_timeout

        # It was Ping before
        if request.request is not None:
            should_add_pending = request.request.is_heartbeat()

            if request.seed:
                state_changed = deepcopy(self.state_changes)
                nodes_ids = [x.member.node_id for x in state_changed]
                for member in self.members.available_nodes():
                    if member.node_id not in nodes_ids:
                        state_changed.append(ArtilleryStateChange(member=member))
            else:
                state_changed = self.state_changes

            message: ArtilleryMessage = build_message(
                self.host_id,
                self.config.cluster_key,
                self.node_type,
                request.request,
                state_changed,
                self.config.network_mtu,
            )

            if should_add_pending:
                self.pending_responses.append(
                    (timeout, request.target, message.state_changes)
                )
            packet_data = message.json(by_alias=True).encode()
            assert len(packet_data) < self.config.network_mtu
            logger.debug(f"{self.host_id} SEND {request.target} {packet_data.decode()}")

            await self.protocol.send(request.target, packet_data)

    def enqueue_seed_nodes(self) -> None:
        for seed_node in self.seed_queue:
            member = self.members.find_member_by_addr(seed_node)
            if member is None or member.member_state == ArtilleryMemberState.Down:
                request = TargetedRequest(target=seed_node)
                logger.debug(f"Equeuing seed {seed_node}")
                request.request = Request.Heartbeat()
                self.request_queue.put_nowait(
                    ArtilleryClusterRequest.React(value=request)
                )

    def enqueue_random_ping(self) -> None:
        member = self.members.next_random_member()
        if member is not None and member.remote_host is not None:
            request = TargetedRequest(target=member.remote_host)
            request.request = Request.Heartbeat(Heartbeat=member.node_id)
            logger.debug(f"Random ping {member.node_id}")
            self.request_queue.put_nowait(ArtilleryClusterRequest.React(value=request))
        if member is not None and member.remote_host is None:
            logger.error(
                "Remote host should not be None on random ping, no target so not send package"
            )

    def prune_timed_out_responses(self) -> None:
        now = time()
        remaining = [resp for resp in self.pending_responses if resp[0] >= now]
        expired_hosts = [resp[1] for resp in self.pending_responses if resp[0] < now]

        self.pending_responses = remaining
        suspect, down = self.members.time_out_nodes(expired_hosts)
        enqueue_state_change(self.state_changes, down)
        enqueue_state_change(self.state_changes, suspect)

        for member in suspect:
            self.send_ping_requests(member)
            self.send_member_event(ArtilleryMemberEvent.SuspectedDown(value=member))

        for member in down:
            self.send_member_event(ArtilleryMemberEvent.WentDown(value=member))

    def send_ping_requests(self, target: ArtilleryMember) -> None:
        target_host = target.remote_host
        if target_host is not None:
            for relay in self.members.hosts_for_indirect_ping(
                self.config.ping_request_host_count, target_host
            ):
                if relay:
                    request = TargetedRequest(target=relay)
                    request.request = Request.Ping(Ping=(target_host, target.node_id))
                    self.request_queue.put_nowait(
                        ArtilleryClusterRequest.React(value=request)
                    )

    async def process_internal_request(
        self, message: ArtilleryClusterRequestTypes
    ) -> None:
        if isinstance(message, ArtilleryClusterRequest.AddSeed):
            self.seed_queue.append(message.value)
        elif isinstance(message, ArtilleryClusterRequest.Respond):
            logger.debug(
                f"{self.host_id} RECV FROM {message.value[0]} {message.value[1]}"
            )
            self.respond_to_message(message.value[0], message.value[1])
        elif isinstance(message, ArtilleryClusterRequest.React):

            self.prune_timed_out_responses()
            await self.process_request(message.value)
        elif isinstance(message, ArtilleryClusterRequest.LeaveCluster):
            myself = self.members.leave()
            enqueue_state_change(self.state_changes, [myself])
        elif isinstance(message, ArtilleryClusterRequest.Payload):
            target_peer: Optional[ArtilleryMember] = self.members.get_member(
                message.value[0]
            )
            if target_peer is not None:
                if not target_peer.is_remote():
                    logger.error("Current node can't send payload to self over LAN")
                    return

                if target_peer.remote_host is not None:
                    request = TargetedRequest(target=target_peer.remote_host)
                    request.request = Request.Payload(
                        id=message.value[0], msg=message.value[1]
                    )
                    await self.process_request(request)
                return
            logger.warn(
                f"Unable to find the peer with an id - {message.value[0]} to send the payload",
            )
        elif isinstance(message, ArtilleryClusterRequest.Exit):
            self.running = False

    def respond_to_message(
        self, src_addr: SocketAddr, message: ArtilleryMessage
    ) -> None:

        if message.cluster_key != self.config.cluster_key:
            logger.error("Mismatching cluster keys, ignore message")
            return

        if not self.members.has_member(src_addr) and self.members.get_member(
            message.node_id
        ):
            logger.warn(
                f"Cannot add member with a node-id {message.node_id}, already present in cluster"
            )
            return

        response: Optional[TargetedRequest] = None

        self.apply_state_changes(message.state_changes, src_addr)
        # remove_potential_seed(self.seed_queue, src_addr)

        self.ensure_node_is_member(src_addr, message.node_id, message.node_type)

        if isinstance(message.request, Request.Heartbeat):
            if (
                message.request.Heartbeat == self.host_id
                or message.request.Heartbeat is None
            ):
                response = TargetedRequest(target=src_addr)
                response.request = Request.Ack(Ack=self.host_id)
                if message.request.Heartbeat is None:
                    response.seed = True
            else:
                logger.info("Heartbeat received not for me")

        elif isinstance(message.request, Request.Ack):
            self.ack_response(src_addr)
            self.mark_node_alive(src_addr, message.request.Ack)
        elif isinstance(message.request, Request.Ping):
            dest_addr = message.request.Ping[0]
            node_id = message.request.Ping[1]
            add_to_wait_list(self.wait_list, dest_addr, src_addr)
            response = TargetedRequest(target=dest_addr)
            response.request = Request.Heartbeat(Heartbeat=node_id)
        elif isinstance(message.request, Request.AckHost):
            member: ArtilleryMember = message.request.AckHost
            if member.remote_host is None:
                raise AttributeError()
            self.ack_response(member.remote_host)
            self.mark_node_alive(member.remote_host, member.node_id)
        elif isinstance(message.request, Request.Payload):
            peer_id = message.request.Payload[0]
            msg = message.request.Payload[1]
            member_payload = self.members.get_member(peer_id)
            if member_payload is not None:
                self.send_member_event(
                    ArtilleryMemberEvent.Payload(Payload=[member_payload, msg])
                )
            else:
                logger.warn(f"Got payload request from an unknown peer {msg}")
            response = None

        if response is not None:
            self.request_queue.put_nowait(ArtilleryClusterRequest.React(value=response))

    def ack_response(self, src_addr: SocketAddr) -> None:
        to_remove: List[Tuple[float, SocketAddr, List[ArtilleryStateChange]]] = []

        for t, addr, state_changes in self.pending_responses:
            if src_addr != addr:
                continue

            to_remove.append((t, addr, state_changes))

            host_keys = [x.member.node_id for x in state_changes]
            self.state_changes = [
                x for x in self.state_changes if x.member.node_id not in host_keys
            ]

        self.pending_responses = [
            x for x in self.pending_responses if x not in to_remove
        ]

    def ensure_node_is_member(
        self, src_addr: SocketAddr, node_id: str, node_type: NodeType = NodeType.Writer
    ) -> None:
        if self.members.has_member(src_addr):
            return
        new_member = ArtilleryMember(
            node_id=node_id,
            remote_host=src_addr,
            incarnation_number=0,
            node_type=node_type,
            member_state=ArtilleryMemberState.Alive,
            last_state_change=time(),
        )

        self.members.add_member(new_member)
        enqueue_state_change(self.state_changes, [new_member])
        self.send_member_event(ArtilleryMemberEvent.Joined(value=new_member))

    def send_member_event(self, event: ArtilleryMemberEventType) -> None:
        if isinstance(event, ArtilleryMemberEvent.Joined):
            pass
        elif isinstance(event, ArtilleryMemberEvent.Payload):
            pass
        elif isinstance(event, ArtilleryMemberEvent.WentUp):
            assert event.value.state() == ArtilleryMemberState.Alive
        elif isinstance(event, ArtilleryMemberEvent.WentDown):
            assert event.value.state() == ArtilleryMemberState.Down
        elif isinstance(event, ArtilleryMemberEvent.SuspectedDown):
            assert event.value.state() == ArtilleryMemberState.Suspect
        elif isinstance(event, ArtilleryMemberEvent.Left):
            assert event.value.state() == ArtilleryMemberState.Left

        # If an error is returned, no one is listening to events anymore. This is normal.
        self.event_queue.put_nowait((self.members.available_nodes(), event))

    def apply_state_changes(
        self, state_changes: List[ArtilleryStateChange], from_: SocketAddr
    ) -> None:
        (new, changed) = self.members.apply_state_changes(state_changes, from_)

        enqueue_state_change(self.state_changes, new)
        enqueue_state_change(self.state_changes, changed)

        for member in new:
            self.send_member_event(ArtilleryMemberEvent.Joined(value=member))

        for member in changed:
            event = determine_member_event(member)
            if event:
                self.send_member_event(event)

    def mark_node_alive(self, src_addr: SocketAddr, node_id: str) -> None:
        member = self.members.mark_node_alive(src_addr, node_id)
        if member is not None:
            wait_list = self.wait_list.get(src_addr)
            if wait_list is not None:
                for remote in wait_list:
                    request = TargetedRequest(target=remote)
                    request.request = Request.AckHost(AckHost=member)
                    self.request_queue.put_nowait(
                        ArtilleryClusterRequest.React(value=request)
                    )

                wait_list.clear()

            enqueue_state_change(self.state_changes, [member])
            self.send_member_event(ArtilleryMemberEvent.WentUp(value=member))


def build_message(
    node_id: str,
    cluster_key: bytes,
    node_type: NodeType,
    request: RequestTypes,
    state_changes: List[ArtilleryStateChange],
    network_mtu: int,
) -> ArtilleryMessage:
    message = ArtilleryMessage(
        node_id=node_id,
        cluster_key=cluster_key,
        node_type=node_type,
        state_changes=[],
    )
    message.request = request

    for i in range(len(state_changes)):
        message = ArtilleryMessage(
            node_id=node_id,
            cluster_key=cluster_key,
            node_type=node_type,
            state_changes=state_changes[: i + 1],
        )
        message.request = request
        encoded = message.json(by_alias=True)

        if len(encoded) >= network_mtu:
            return message
    return message


def add_to_wait_list(
    wait_list: Dict[SocketAddr, List[SocketAddr]],
    wait_addr: SocketAddr,
    notify_addr: SocketAddr,
) -> None:
    wait_list.setdefault(wait_addr, []).append(notify_addr)


def determine_member_event(
    member: ArtilleryMember,
) -> Optional[ArtilleryMemberEventType]:
    if member.state() == ArtilleryMemberState.Alive:
        return ArtilleryMemberEvent.WentUp(value=member)
    elif member.state() == ArtilleryMemberState.Suspect:
        return ArtilleryMemberEvent.SuspectedDown(value=member)
    elif member.state() == ArtilleryMemberState.Down:
        return ArtilleryMemberEvent.WentDown(value=member)
    elif member.state() == ArtilleryMemberState.Left:
        return ArtilleryMemberEvent.Left(value=member)
    return None


def enqueue_state_change(
    state_changes: List[ArtilleryStateChange],
    members: List[ArtilleryMember],
) -> None:
    for member in members:
        for state_change in state_changes:
            if state_change.member.node_id == member.node_id:
                state_change.member = member
                return

        state_changes.append(ArtilleryStateChange(member=member))
