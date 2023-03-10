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

from typing import List, Optional
from urllib.parse import urlparse
from uuid import uuid4

import requests

from nucliadb_models.resource import KnowledgeBoxList, KnowledgeBoxObj
from nucliadb_models.utils import SlugString
from nucliadb_sdk.client import Environment, NucliaDBClient
from nucliadb_sdk.knowledgebox import KnowledgeBox


class InvalidHost(Exception):
    pass


class KnowledgeBoxAlreadyExists(Exception):
    pass


def create_knowledge_box(
    slug: Optional[str] = None,
    nucliadb_base_url: Optional[str] = "http://localhost:8080",
    similarity: Optional[str] = None,
):
    url_obj = urlparse(nucliadb_base_url)
    if url_obj.hostname and url_obj.hostname.endswith("nuclia.cloud"):  # type: ignore
        raise InvalidHost(
            "You can not create a Knowledge Box via API, please use https://nuclia.cloud interface"
        )

    if slug is None:
        slug = uuid4().hex

    payload = {"slug": slug}
    if similarity:
        payload["similarity"] = similarity

    api_path = f"{nucliadb_base_url}/api/v1"
    response = requests.post(
        f"{api_path}/kbs",
        json=payload,
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
    )
    if response.status_code == 419:
        raise KnowledgeBoxAlreadyExists()

    assert response.status_code == 201

    kb = KnowledgeBoxObj.parse_raw(response.content)
    kbid = kb.uuid

    url = f"{api_path}/kb/{kbid}"
    client = NucliaDBClient(environment=Environment.OSS, url=url)

    return KnowledgeBox(client)


def get_kb(
    slug: str,
    nucliadb_base_url: Optional[str] = "http://localhost:8080",
) -> Optional[KnowledgeBox]:
    url_obj = urlparse(nucliadb_base_url)
    if url_obj.hostname and url_obj.hostname.endswith("nuclia.cloud"):  # type: ignore
        raise InvalidHost(
            "You can not create a Knowledge Box via API, please use https://nuclia.cloud interface"
        )

    api_path = f"{nucliadb_base_url}/api/v1"
    response = requests.get(
        f"{api_path}/kb/s/{slug}",
        headers={"X-NUCLIADB-ROLES": "READER"},
    )

    if response.status_code == 404:
        return None

    assert response.status_code == 200

    kb = KnowledgeBoxObj.parse_raw(response.content)
    kbid = kb.uuid

    url = f"{api_path}/kb/{kbid}"
    client = NucliaDBClient(environment=Environment.OSS, url=url)

    return KnowledgeBox(client)


def get_or_create(
    slug: str,
    nucliadb_base_url: Optional[str] = "http://localhost:8080",
    similarity: Optional[str] = None,
):
    kb = get_kb(slug, nucliadb_base_url)
    if kb is None:
        kb = create_knowledge_box(
            slug, nucliadb_base_url=nucliadb_base_url, similarity=similarity
        )
    return kb


def delete_kb(slug: str, nucliadb_base_url: Optional[str] = "http://localhost:8080"):
    kb = get_kb(slug, nucliadb_base_url)
    if kb is None or kb.client.url is None:
        raise AttributeError("URL should not be none")
    response = requests.delete(kb.client.url, headers={"X-NUCLIADB-ROLES": f"MANAGER"})
    assert response.status_code == 200


def list_kbs(
    nucliadb_base_url: Optional[str] = "http://localhost:8080",
) -> List[Optional[SlugString]]:
    response = requests.get(
        f"{nucliadb_base_url}/kbs",
        headers={"X-NUCLIADB-ROLES": "MANAGER"},
    )

    assert response.status_code == 200
    kbs = KnowledgeBoxList.parse_raw(response.content)
    return [kb.slug for kb in kbs.kbs]
