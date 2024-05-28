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
from datetime import datetime

from nucliadb_protos.writer_pb2 import Audit
from starlette.requests import Request


def parse_audit(audit: Audit, request: Request):
    audit.user = request.headers.get("X-NUCLIADB-USER", "")

    audit.when.FromDatetime(datetime.now())

    agents = request.headers.get("USER-AGENT", "").lower()
    if "dashboard" in agents:
        audit.source = Audit.Source.HTTP
    elif "desktop" in agents:
        audit.source = Audit.Source.DESKTOP
    else:
        audit.source = Audit.Source.HTTP

    audit.origin = request.headers.get("X-FORWARDED-FOR", "")
