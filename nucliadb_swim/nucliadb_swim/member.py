from __future__ import annotations

from enum import Enum
from time import time
from typing import Optional

from pydantic import BaseModel, Field

from nucliadb_swim.protocol import SocketAddr


class ArtilleryMemberState(str, Enum):
    Alive = "a"
    Suspect = "s"
    Down = "d"
    Left = "l"


class NodeType(str, Enum):
    Node = "N"
    Writer = "W"
    Reader = "R"
    Search = "S"


class ArtilleryMember(BaseModel):
    node_id: str = Field(..., alias="h")
    remote_host: Optional[SocketAddr] = Field(..., alias="r")
    incarnation_number: int = Field(..., alias="i")
    node_type: NodeType = Field(..., alias="l")
    member_state: ArtilleryMemberState = Field(..., alias="m")
    last_state_change: float = Field(default_factory=time, alias="t")

    class Config:
        allow_population_by_field_name = True

    @classmethod
    def current(
        cls, host_key: str, node_type: NodeType = NodeType.Writer
    ) -> ArtilleryMember:
        return ArtilleryMember(
            node_id=host_key,
            remote_host=None,
            incarnation_number=0,
            node_type=node_type,
            member_state=ArtilleryMemberState.Alive,
        )

    def is_current(self) -> bool:
        return self.remote_host is None

    def is_remote(self) -> bool:
        return self.remote_host is not None

    def state_change_older_than(self, duration: int) -> bool:
        return self.last_state_change + duration < time()

    def state(self) -> ArtilleryMemberState:
        return self.member_state

    def set_state(self, state: ArtilleryMemberState) -> None:
        if self.member_state != state:
            self.member_state = state
            self.last_state_change = time()

    def reincarnate(self) -> None:
        self.incarnation_number += 1

    def member_by_changing_host(self, remote_host: SocketAddr) -> ArtilleryMember:
        am = ArtilleryMember(
            node_id=self.node_id,
            incarnation_number=self.incarnation_number,
            node_type=self.node_type,
            member_state=self.member_state,
            remote_host=remote_host,
            last_state_change=self.last_state_change,
        )
        return am

    def __cmp__(self, other: ArtilleryMember) -> bool:
        t1 = (
            self.node_id,
            self.remote_host,
            self.incarnation_number,
            self.member_state.value,
        )
        t2 = (
            other.node_id,
            other.remote_host,
            other.incarnation_number,
            other.member_state.value,
        )

        return t1 > t2

    def __repr__(self) -> str:
        return f"<{self.node_id}/{self.node_type} {self.remote_host} \
                  {self.incarnation_number} {self.state()}>"


def most_uptodate_member_data(
    lhs: ArtilleryMember, rhs: ArtilleryMember
) -> ArtilleryMember:
    lhs_overrides = False
    if lhs.incarnation_number > rhs.incarnation_number:
        if lhs.member_state in (
            ArtilleryMemberState.Suspect,
            ArtilleryMemberState.Alive,
        ) and rhs.member_state in (
            ArtilleryMemberState.Suspect,
            ArtilleryMemberState.Alive,
        ):
            lhs_overrides = True
    elif lhs.member_state == ArtilleryMemberState.Left:
        lhs_overrides = True
    elif lhs.member_state == ArtilleryMemberState.Down and rhs.member_state in (
        ArtilleryMemberState.Suspect,
        ArtilleryMemberState.Alive,
    ):
        lhs_overrides = True

    if lhs_overrides:
        return lhs
    else:
        return rhs


class ArtilleryStateChange(BaseModel):
    member: ArtilleryMember
