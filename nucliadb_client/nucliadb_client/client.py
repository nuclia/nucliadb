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

import asyncio
import tempfile
from io import StringIO
from typing import Any, List, Optional, Union

import aiofiles
import httpx
from grpc import aio  # type: ignore
from grpc import insecure_channel
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.writer_pb2_grpc import WriterStub
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat

from nucliadb_client import logger
from nucliadb_client.exceptions import ConflictError
from nucliadb_client.knowledgebox import KnowledgeBox
from nucliadb_models.resource import (
    KnowledgeBoxConfig,
    KnowledgeBoxList,
    KnowledgeBoxObj,
)
from nucliadb_telemetry.grpc import OpenTelemetryGRPC
from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.utils import create_telemetry

API_PREFIX = "api"
KBS_PREFIX = "/kbs"
KB_PREFIX = "/kb"

SERVICE_NAME = "nucliadb_client"


class NucliaDBClient:
    writer_stub_async: Optional[WriterStub] = None
    writer_channel_async: Optional[Any] = None

    def __init__(
        self,
        host: str,
        grpc: int,
        http: int,
        train: Optional[int] = None,
        schema: str = "http",
        writer_host: Optional[str] = None,
        reader_host: Optional[str] = None,
        search_host: Optional[str] = None,
        grpc_host: Optional[str] = None,
        train_host: Optional[str] = None,
    ):
        if reader_host is None:
            reader_host = host
        if writer_host is None:
            writer_host = host
        if search_host is None:
            search_host = host
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
        self.http_search_v1 = httpx.Client(
            base_url=f"{schema}://{search_host}:{http}/{API_PREFIX}/v1",
            headers={"X-NUCLIADB-ROLES": "READER"},
        )
        self.http_manager_v1 = httpx.Client(
            base_url=f"{schema}://{host}:{http}/{API_PREFIX}/v1",
            headers={"X-NUCLIADB-ROLES": "MANAGER"},
        )
        if grpc_host is None:
            grpc_host = host

        self.grpc_host = grpc_host
        self.grpc_port = grpc
        channel = insecure_channel(f"{host}:{grpc}")
        self.writer_stub = WriterStub(channel)
        self.train_stub = None
        if train and train_host:
            self.train_stub = TrainStub(insecure_channel(f"{train_host}:{train}"))

    def list_kbs(self, timeout: Optional[int] = None) -> List[KnowledgeBox]:
        response = KnowledgeBoxList.parse_raw(
            self.http_reader_v1.get(
                KBS_PREFIX, timeout=timeout, headers={"X-NUCLIADB-ROLES": "MANAGER"}
            ).content
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

    async def export_kb(
        self,
        *,
        kbid: str,
        location: str,
    ) -> None:
        kb = self.get_kb(kbid=kbid)
        if kb is None:
            raise KeyError(f"KB {kbid} not found")
        await kb.export(location)

    async def import_kb(
        self,
        *,
        kbid: Optional[str] = None,
        slug: Optional[str] = None,
        location: Union[str, StringIO],
    ) -> str:
        self.init_async_grpc()
        if slug is None and kbid is None:
            raise AttributeError("Either slug or kbid needs to be set")
        if kbid:
            kb = self.get_kb(kbid=kbid)
            if kb is None:
                raise AttributeError("With kbid KB needs to be already created")
        else:
            kb = self.get_kb(slug=slug)
            if kb is None:
                kb = self.create_kb(slug=slug)  # type: ignore

        logger.info("Importing Binaries")
        if isinstance(location, StringIO):
            logger.info("No Binaries import on STRINGIO")

        elif location.startswith("http"):
            tar_location = location + ".tar.bz2"
            client = httpx.AsyncClient()
            resp = await client.get(tar_location)
            assert resp.status_code == 200
            with tempfile.NamedTemporaryFile(suffix=".tar.bz2") as temp:
                for chunk in resp.iter_bytes():
                    temp.write(chunk)
                temp.flush()
                await kb.import_tar_bz2(temp.name)
        else:
            tar_location = location + ".tar.bz2"
            await kb.import_tar_bz2(tar_location)

        logger.info("Importing MainDB")
        if isinstance(location, StringIO):
            b64_pb = location.readline()
            while b64_pb:
                await kb.import_export(b64_pb.strip())
                b64_pb = location.readline()

        elif location.startswith("http"):
            client = httpx.AsyncClient()
            resp = await client.get(location)
            assert resp.status_code == 200
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
            logger.warning("Exists already a writer, replacing on the new loop")
            asyncio.ensure_future(self.writer_channel_async.close())

        max_send_message = 1024
        grpc_addr = f"{self.grpc_host}:{self.grpc_port}"
        if telemetry_settings.jaeger_enabled:
            tracer_provider = create_telemetry(SERVICE_NAME)
            telemetry_grpc = OpenTelemetryGRPC(SERVICE_NAME, tracer_provider)
            set_global_textmap(B3MultiFormat())
            channel = telemetry_grpc.init_client(
                grpc_addr, max_send_message=max_send_message
            )
        else:
            options = [
                ("grpc.max_receive_message_length", max_send_message * 1024 * 1024),
            ]
            channel = aio.insecure_channel(grpc_addr, options)
        self.writer_stub_async = WriterStub(channel)
        self.writer_channel_async = channel

    async def finish_async_grpc(self):
        if self.writer_stub_async is None:
            logger.warning("Writer does not exist")
        self.writer_stub_async = None
        await self.writer_channel_async.close()
