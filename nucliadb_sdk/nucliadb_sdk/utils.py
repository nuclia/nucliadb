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

# europe-1.nuclia.cloud
# localhost:8080

from typing import Optional
from urllib.parse import urlparse
from uuid import uuid4

import requests

from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_sdk.client import Environment, NucliaDBClient
from nucliadb_sdk.knowledgebox import KnowledgeBox


class InvalidHost(Exception):
    pass


def create_knowledge_box(
    nucliadb_base_url: Optional[str] = "http://localhost:8080",
    slug: Optional[str] = None,
):
    url_obj = urlparse(nucliadb_base_url)
    if url_obj.hostname and url_obj.hostname.endswith("nuclia.cloud"):  # type: ignore
        raise InvalidHost(
            "You can not create a Knowledge Box via API, please use https://nuclia.cloud interface"
        )

    if slug is None:
        slug = uuid4().hex

    api_path = f"{nucliadb_base_url}/api/v1"
    response = requests.post(
        f"{api_path}/kbs",
        json={"slug": slug},
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
    )
    assert response.status_code == 201

    kb = KnowledgeBoxObj.parse_raw(response.content)
    kbid = kb.uuid

    url = f"{api_path}/kb/{kbid}"
    client = NucliaDBClient(environment=Environment.OSS, url=url)

    return KnowledgeBox(client)
