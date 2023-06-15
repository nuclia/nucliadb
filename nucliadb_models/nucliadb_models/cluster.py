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
from enum import Enum
from typing import Optional

from nucliadb_protos.writer_pb2 import Member
from pydantic import BaseModel, Field


class MemberType(str, Enum):
    IO = "Io"
    SEARCH = "Search"
    INGEST = "Ingest"
    TRAIN = "Train"
    UNKNOWN = "Unknown"

    @staticmethod
    def from_pb(node_type: Member.Type.ValueType):
        if node_type == Member.Type.IO:
            return MemberType.IO
        elif node_type == Member.Type.SEARCH:
            return MemberType.SEARCH
        elif node_type == Member.Type.INGEST:
            return MemberType.INGEST
        elif node_type == Member.Type.TRAIN:
            return MemberType.TRAIN
        elif node_type == Member.Type.UNKNOWN:
            return MemberType.UNKNOWN
        else:
            raise ValueError(f"incompatible node type '{node_type}'")

    def to_pb(self) -> Member.Type.ValueType:
        if self == MemberType.IO:
            return Member.Type.IO
        elif self == MemberType.SEARCH:
            return Member.Type.SEARCH
        elif self == MemberType.INGEST:
            return Member.Type.INGEST
        elif self == MemberType.TRAIN:
            return Member.Type.TRAIN
        else:
            return Member.Type.UNKNOWN


class ClusterMember(BaseModel):
    node_id: str = Field(alias="id")
    listen_addr: str = Field(alias="address")
    type: MemberType
    is_self: bool
    shard_count: Optional[int]

    class Config:
        allow_population_by_field_name = True
