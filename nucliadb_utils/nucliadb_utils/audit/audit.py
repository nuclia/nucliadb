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

from nucliadb_protos.audit_pb2 import AuditField, AuditRequest
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.writer_pb2 import BrokerMessage


class AuditStorage:
    async def report(self, message: BrokerMessage, audit_type: AuditRequest.AuditType.Value, audit_fields: Optional[List[AuditField]] = None):  # type: ignore

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
        origin: str,
        search: SearchRequest,
        timeit: float,
        resources: int,
    ):
        raise NotImplementedError

    async def delete_kb(self, kbid):
        raise NotImplementedError
