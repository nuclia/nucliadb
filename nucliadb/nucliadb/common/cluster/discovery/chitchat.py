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
from __future__ import annotations

import asyncio
from enum import Enum
from typing import Optional

import pydantic
from fastapi import APIRouter, FastAPI, Response
from nucliadb_protos.writer_pb2 import Member
from uvicorn.config import Config  # type: ignore
from uvicorn.server import Server  # type: ignore

from nucliadb.common.cluster.discovery.abc import (
    AbstractClusterDiscovery,
    update_members,
)
from nucliadb.common.cluster.discovery.types import IndexNodeMetadata
from nucliadb.common.cluster.settings import Settings
from nucliadb.ingest import logger
from nucliadb_utils.fastapi.run import start_server

api_router = APIRouter()


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


class ClusterMember(pydantic.BaseModel):
    node_id: str = pydantic.Field(alias="id")
    listen_addr: str = pydantic.Field(alias="address")
    shard_count: Optional[int]
    type: MemberType = MemberType.UNKNOWN
    is_self: bool = False

    class Config:
        allow_population_by_field_name = True


@api_router.patch("/members", status_code=204)
async def api_update_members(members: list[ClusterMember]) -> Response:
    update_members(
        [
            IndexNodeMetadata(
                node_id=member.node_id,
                name=member.node_id,
                address=member.listen_addr,
                shard_count=member.shard_count or 0,
            )
            for member in members
            if not member.is_self and member.type == MemberType.IO
        ]
    )
    return Response(status_code=204)


class ChitchatAutoDiscovery(AbstractClusterDiscovery):
    """
    This is starting a HTTP server that will receives periodic chitchat-cluster
    member changes and it will update the in-memory list of available nodes.
    """

    def __init__(self, settings: Settings):
        super().__init__(settings)

        self.app = FastAPI(title="Chitchat monitor server")
        self.app.include_router(api_router)

        self.config = Config(
            self.app,
            host=self.settings.chitchat_binding_host,
            port=self.settings.chitchat_binding_port,
            debug=False,
            loop="auto",
            http="auto",
            reload=False,
            workers=1,
            use_colors=False,
            log_config=None,
            limit_concurrency=None,
            backlog=2047,
            limit_max_requests=None,
            timeout_keep_alive=5,
        )
        self.server = Server(config=self.config)

    async def initialize(self) -> None:
        logger.info(
            f"Chitchat server started at: {self.settings.chitchat_binding_host}:{self.settings.chitchat_binding_port}"
        )
        self.task = asyncio.create_task(start_server(self.server, self.config))

    async def finalize(self) -> None:
        logger.info("Chitchat closed")
        try:
            await self.server.shutdown()
        except AttributeError:  # pragma: no cover
            # Problem with uvicorn that can happen in tests
            pass
        self.task.cancel()
