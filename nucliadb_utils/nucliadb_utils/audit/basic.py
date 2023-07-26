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
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_utils import logger
from nucliadb_utils.audit.audit import AuditStorage


class BasicAuditStorage(AuditStorage):
    def message_to_str(self, message: BrokerMessage) -> str:
        return f"{message.type}+{message.multiid}+{message.audit.user}+{message.kbid}+{message.uuid}+{message.audit.when.ToJsonString()}+{message.audit.origin}+{message.audit.source}"  # noqa

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
        logger.debug(
            f"AUDIT {audit_type} {kbid} {user} {origin} {rid} {audit_fields} {kb_counter}"
        )

    async def visited(self, kbid: str, uuid: str, user: str, origin: str):
        logger.debug(f"VISITED {kbid} {uuid} {user} {origin}")

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
        logger.debug(f"SEARCH {kbid} {user} {origin} ''{search}'' {timeit} {resources}")

    async def suggest(
        self,
        kbid: str,
        user: str,
        client: int,
        origin: str,
        timeit: float,
    ):
        logger.debug(f"SUGGEST {kbid} {user} {origin} {timeit}")

    async def chat(
        self,
        kbid: str,
        user: str,
        client_type: int,
        origin: str,
        timeit: float,
        question: str,
        rephrased_question: Optional[str],
        context: List[ChatContext],
        answer: Optional[str],
    ):
        logger.debug(f"CHAT {kbid} {user} {origin} {timeit}")

    async def delete_kb(self, kbid):
        logger.debug(f"KB DELETED {kbid}")
