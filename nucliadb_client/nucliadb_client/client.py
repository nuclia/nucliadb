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

from typing import List, Optional

import httpx
from grpc import insecure_channel
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb_client.knowledgebox import KnowledgeBox
from nucliadb_models.resource import (
    KnowledgeBoxConfig,
    KnowledgeBoxList,
    KnowledgeBoxObj,
)

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
        train: int,
        schema: str = "http",
        writer_host: Optional[str] = None,
        reader_host: Optional[str] = None,
        grpc_host: Optional[str] = None,
    ):
        if reader_host is None:
            reader_host = host
        if writer_host is None:
            writer_host = host
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

    def get_kb(self, *, slug: str) -> Optional[KnowledgeBox]:
        response = self.http_reader_v1.get(f"{KB_PREFIX}/s/{slug}")
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
        response_obj = KnowledgeBoxObj.parse_raw(response.content)
        return KnowledgeBox(kbid=response_obj.uuid, client=self, slug=response_obj.slug)
