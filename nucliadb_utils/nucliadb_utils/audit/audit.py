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
from typing import List, Optional

from google.protobuf.timestamp_pb2 import Timestamp
from nucliadb_protos.audit_pb2 import (
    AuditField,
    AuditKBCounter,
    AuditRequest,
    ChatContext,
)
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.resources_pb2 import FieldID


class AuditStorage:
    async def report(
        self,
        *,
        kbid: str,
        audit_type: AuditRequest.AuditType.Value,  # type: ignore
        when: Optional[Timestamp] = None,
        user: Optional[str] = None,
        origin: Optional[str] = None,
        rid: Optional[str] = None,
        field_metadata: Optional[List[FieldID]] = None,
        audit_fields: Optional[List[AuditField]] = None,
        kb_counter: Optional[AuditKBCounter] = None,
    ):  # type: ignore
        raise NotImplementedError

    async def initialize(self):
        pass

    async def finalize(self):
        pass

    async def visited(self, kbid: str, uuid: str, user: str, origin: str):
        raise NotImplementedError

    async def search(
        self,
        kbid: str,
        user: str,
        client: int,
        origin: str,
        search: SearchRequest,
        timeit: float,
        resources: int,
    ):
        raise NotImplementedError

    async def suggest(
        self,
        kbid: str,
        user: str,
        client: int,
        origin: str,
        timeit: float,
    ):
        raise NotImplementedError

    async def chat(
        self,
        kbid: str,
        user: str,
        client: int,
        origin: str,
        timeit: float,
        question: str,
        rephrased_question: Optional[str],
        context: List[ChatContext],
        answer: Optional[str],
    ):
        raise NotImplementedError

    async def delete_kb(self, kbid):
        raise NotImplementedError
