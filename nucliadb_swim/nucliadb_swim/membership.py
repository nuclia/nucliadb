import random
from typing import List, Optional, Tuple

from nucliadb_swim.member import (
    ArtilleryMember,
    ArtilleryMemberState,
    ArtilleryStateChange,
    most_uptodate_member_data,
)
from nucliadb_swim.protocol import SocketAddr


class ArtilleryMemberList:
    members: List[ArtilleryMember]
    periodic_index: int

    def __init__(self, current: ArtilleryMember):
        self.members = [current]
        self.periodic_index = 0

    def available_nodes(self) -> List[ArtilleryMember]:
        return [
            member
            for member in self.members
            if member.state() != ArtilleryMemberState.Left
        ]

    def current_node_id(self) -> Optional[str]:
        for member in self.members:
            if member.is_current():
                return member.node_id
        return None

    def myself(self) -> ArtilleryMember:
        for member in self.members:
            if member.is_current():
                return member
        raise Exception("Could not find this instance as registered member")

    def reincarnate_self(self) -> ArtilleryMember:
        member = self.myself()
        member.reincarnate()
        return member

    def leave(self) -> ArtilleryMember:
        member = self.myself()
        member.set_state(ArtilleryMemberState.Left)
        member.reincarnate()
        return member

    def next_random_member(self) -> Optional[ArtilleryMember]:
        if self.periodic_index == 0:
            random.shuffle(self.members)

        other_members = [member for member in self.members if member.is_remote()]

        if len(other_members) == 0:
            return None
        else:
            self.periodic_index = (self.periodic_index + 1) % len(other_members)
            return other_members[self.periodic_index]

    def time_out_nodes(
        self, expired_hosts: List[SocketAddr]
    ) -> Tuple[List[ArtilleryMember], List[ArtilleryMember]]:
        suspect_members = []
        down_members = []
        for member in self.members:
            if member.remote_host is not None and member.remote_host in expired_hosts:
                if member.state() == ArtilleryMemberState.Alive:
                    member.set_state(ArtilleryMemberState.Suspect)
                    suspect_members.append(member)
                elif member.state() == ArtilleryMemberState.Suspect:
                    if member.state_change_older_than(3):
                        member.set_state(ArtilleryMemberState.Down)
                        down_members.append(member)
        return suspect_members, down_members

    def set_node_id(self, src_addr: SocketAddr, node_id: str):
        member = self.get_member_for_host(src_addr)
        if member:
            member.node_id = node_id

    def mark_node_alive(
        self, src_addr: SocketAddr, node_id: str
    ) -> Optional[ArtilleryMember]:
        if self.current_node_id() == node_id:
            return None
        self.set_node_id(src_addr, node_id)
        for member in self.members:
            if (
                member.remote_host == src_addr
                and member.state() != ArtilleryMemberState.Alive
            ):
                member.set_state(ArtilleryMemberState.Alive)
                return member
        return None

    def find_member(
        self,
        members: List[ArtilleryMember],
        node_id: str,
        addr: SocketAddr,
    ) -> Optional[ArtilleryMember]:
        matching_node_id: Optional[ArtilleryMember] = next(
            filter(lambda x: x.node_id == node_id, members), None  # type: ignore
        )
        matching_address: Optional[ArtilleryMember] = next(
            filter(lambda x: x.remote_host == addr, members), None  # type: ignore
        )
        choosen = matching_node_id or matching_address
        return choosen

    def find_member_by_addr(
        self,
        addr: SocketAddr,
    ) -> Optional[ArtilleryMember]:
        matching_address: Optional[ArtilleryMember] = next(
            filter(lambda x: x.remote_host == addr, self.members), None  # type: ignore
        )
        return matching_address

    def apply_state_changes(
        self, state_changes: List[ArtilleryStateChange], from_: SocketAddr
    ) -> Tuple[List[ArtilleryMember], List[ArtilleryMember]]:
        current_members = self.members
        changed_nodes = []
        new_nodes = []

        my_node_id = self.current_node_id()

        for state_change in state_changes:
            member_change = state_change.member

            if member_change.node_id == my_node_id:
                # Myself now is alive
                if member_change.state() != ArtilleryMemberState.Alive:
                    myself = self.reincarnate_self()
                    changed_nodes.append(myself)

            else:
                existing_member = self.find_member(
                    current_members,
                    member_change.node_id,
                    member_change.remote_host or from_,
                )
                if existing_member:
                    update_member = most_uptodate_member_data(
                        member_change, existing_member
                    )
                    new_host = update_member.remote_host or existing_member.remote_host
                    if new_host is None:
                        raise AttributeError("New host should not be empty ever")
                    update_member = update_member.member_by_changing_host(new_host)
                    if update_member.state() != existing_member.state():
                        existing_member.set_state(update_member.state())
                        existing_member.incarnation_number = (
                            update_member.incarnation_number
                        )
                        if update_member.remote_host:
                            existing_member.remote_host = update_member.remote_host
                        changed_nodes.append(update_member)

                elif (
                    member_change.state() != ArtilleryMemberState.Down
                    and member_change.state() != ArtilleryMemberState.Left
                ):
                    new_host = member_change.remote_host or from_
                    if new_host.__class__ == str:
                        new_host = SocketAddr(new_host)
                    new_member = member_change.member_by_changing_host(new_host)
                    current_members.append(new_member)
                    new_nodes.append(new_member)

        self.members = [
            x
            for x in current_members
            if x.state() != ArtilleryMemberState.Down
            and x.state() != ArtilleryMemberState.Left
        ]
        return (new_nodes, changed_nodes)

    def hosts_for_indirect_ping(
        self, host_count: int, target: SocketAddr
    ) -> List[Optional[SocketAddr]]:
        possible_members = [
            m.remote_host
            for m in self.members
            if m.state() == ArtilleryMemberState.Alive
            and m.is_remote()
            and m.remote_host != target
        ]
        random.shuffle(possible_members)
        return possible_members[:host_count]

    def has_member(self, remote_host: SocketAddr) -> bool:
        for member in self.members:
            if member.remote_host == remote_host:
                return True
        return False

    def add_member(self, member: ArtilleryMember) -> None:
        self.members.append(member)

    def remove_member(self, id: str):
        self.members = [x for x in self.members if x.node_id != id]

    def get_member_for_host(self, host: SocketAddr) -> Optional[ArtilleryMember]:
        for member in self.members:
            if member.remote_host == host:
                return member
        return None

    def get_member(self, id: str) -> Optional[ArtilleryMember]:
        for member in self.members:
            if member.node_id == id:
                return member
        return None
