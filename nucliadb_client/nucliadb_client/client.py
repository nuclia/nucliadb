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

from io import StringIO
from typing import List, Optional, Union

import aiofiles
import httpx
from grpc import aio  # type: ignore
from grpc import insecure_channel
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.models.resource import (
    KnowledgeBoxConfig,
    KnowledgeBoxList,
    KnowledgeBoxObj,
)
from nucliadb_client import logger
from nucliadb_client.exceptions import ConflictError
from nucliadb_client.knowledgebox import KnowledgeBox

API_PREFIX = "api"
KBS_PREFIX = "/kbs"
KB_PREFIX = "/kb"


class NucliaDBClient:
    writer_stub_async: Optional[WriterStub] = None

    def __init__(
        self,
        host: str,
        grpc: int,
        http: int,
        train: Optional[int] = None,
        schema: str = "http",
        writer_host: Optional[str] = None,
        reader_host: Optional[str] = None,
        grpc_host: Optional[str] = None,
        train_host: Optional[str] = None,
    ):
        if reader_host is None:
            reader_host = host
        if writer_host is None:
            writer_host = host
        if train_host is None:
            train_host = host
        self.http_reader_v1 = httpx.Client(
            base_url=f"{schema}://{reader_host}:{http}/{API_PREFIX}/v1",
            headers={"X-NUCLIADB-ROLES": "READER"},
        )
        self.http_writer_v1 = httpx.Client(
            base_url=f"{schema}://{writer_host}:{http}/{API_PREFIX}/v1",
            headers={"X-NUCLIADB-ROLES": "WRITER"},
        )
        self.http_manager_v1 = httpx.Client(
            base_url=f"{schema}://{host}:{http}/{API_PREFIX}/v1",
            headers={"X-NUCLIADB-ROLES": "MANAGER"},
        )
        if grpc_host is None:
            grpc_host = host
        self.grpc_host = grpc_host
        self.grpc_port = grpc
        self.train_port = train
        self.train_host = train_host
        channel = insecure_channel(f"{host}:{grpc}")
        self.writer_stub = WriterStub(channel)

    def list_kbs(self) -> List[KnowledgeBox]:
        response = KnowledgeBoxList.parse_raw(
            self.http_manager_v1.get(KBS_PREFIX).content
        )
        result = []
        for kb in response.kbs:
            new_kb = KnowledgeBox(kbid=kb.uuid, client=self, slug=kb.slug)
            result.append(new_kb)
        return result

    def get_kb(
        self, *, slug: Optional[str] = None, kbid: Optional[str] = None
    ) -> Optional[KnowledgeBox]:
        if slug is None and kbid is None:
            raise ValueError("Either slug or kbid must be set")

        if slug:
            url = f"{KB_PREFIX}/s/{slug}"
        else:
            url = f"{KB_PREFIX}/{kbid}"

        response = self.http_reader_v1.get(url)
        if response.status_code == 404:
            return None

        response_obj = KnowledgeBoxObj.parse_raw(response.content)
        return KnowledgeBox(kbid=response_obj.uuid, client=self, slug=response_obj.slug)

    def create_kb(
        self,
        *,
        slug: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
    ) -> KnowledgeBox:
        payload = KnowledgeBoxConfig()
        payload.slug = slug  # type: ignore
        payload.title = title
        payload.description = description

        response = self.http_manager_v1.post(KBS_PREFIX, json=payload.dict())
        if response.status_code == 419:
            raise ConflictError()
        response_obj = KnowledgeBoxObj.parse_raw(response.content)
        return KnowledgeBox(kbid=response_obj.uuid, client=self, slug=response_obj.slug)

    async def import_kb(self, *, slug: str, location: Union[str, StringIO]) -> str:
        self.init_async_grpc()
        kb = self.get_kb(slug=slug)
        if kb is None:
            kb = self.create_kb(slug=slug)

        if isinstance(location, StringIO):
            b64_pb = location.readline()
            while b64_pb:
                await kb.import_export(b64_pb.strip())
                b64_pb = location.readline()

        elif location.startswith("http"):
            client = httpx.AsyncClient()
            resp = await client.get(location)
            async for line in resp.aiter_lines():
                await kb.import_export(line.strip())

        else:
            async with aiofiles.open(location, "r") as dump_file:

                b64_pb = await dump_file.readline()
                while b64_pb:
                    await kb.import_export(b64_pb.strip())
                    b64_pb = await dump_file.readline()
        return kb.kbid

    def init_async_grpc(self):
        if self.writer_stub_async is not None:
            logger.warn("Exists already a writer, replacing on the new loop")
        options = [
            ("grpc.max_receive_message_length", 1024 * 1024 * 1024),
        ]
        async_channel = aio.insecure_channel(
            f"{self.grpc_host}:{self.grpc_port}", options
        )
        self.writer_stub_async = WriterStub(async_channel)
