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

from dataclasses import dataclass

from nucliadb_sdk import NucliaDB
from nucliadb_sdk.v2.exceptions import NotFoundError

LOCAL_API = "http://localhost:8080/api"
CLUSTER_API = "http://{service}.nucliadb.svc.cluster.local:8080/api"
ROLES_HEADER = "READER;WRITER;MANAGER"


@dataclass
class NucliaDBClient:
    reader: NucliaDB
    writer: NucliaDB


def get_nucliadb_client(local: bool = True) -> NucliaDBClient:
    if local:
        return NucliaDBClient(
            reader=NucliaDB(url=LOCAL_API, headers={"X-Nucliadb-Roles": ROLES_HEADER}),
            writer=NucliaDB(url=LOCAL_API, headers={"X-Nucliadb-Roles": ROLES_HEADER}),
        )
    return NucliaDBClient(
        reader=NucliaDB(
            url=CLUSTER_API.format(service="reader"),
            headers={"X-Nucliadb-Roles": ROLES_HEADER},
        ),
        writer=NucliaDB(
            url=CLUSTER_API.format(service="writer"),
            headers={"X-Nucliadb-Roles": ROLES_HEADER},
        ),
    )


def get_kbid(ndb, slug_or_kbid) -> str:
    try:
        kbid = ndb.reader.get_knowledge_box_by_slug(slug=slug_or_kbid).uuid
    except NotFoundError:
        kbid = ndb.reader.get_knowledge_box(kbid=slug_or_kbid)
    return kbid
