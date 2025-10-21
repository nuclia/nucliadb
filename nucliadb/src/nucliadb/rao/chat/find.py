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
from httpx import AsyncClient

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.search import API_PREFIX
from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb.search.search.metrics import Metrics
from nucliadb_models.search import (
    FindRequest,
    KnowledgeboxFindResults,
    NucliaDBClientType,
)
from nucliadb_utils.settings import running_settings


async def find(
    kbid: str,
    item: FindRequest,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
    # XXX: we are losing track of metrics ignoring this. Do we care?
    metrics: Metrics,
) -> tuple[KnowledgeboxFindResults, bool]:
    """RPC to /find endpoint making it look as an internal call."""

    async with AsyncClient(
        headers={"X-NUCLIADB-ROLES": "READER"},
        base_url=f"http://{running_settings.serving_host}:{running_settings.serving_port}/{API_PREFIX}/v1",
        timeout=10.0,
    ) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/find",
            headers={
                "x-ndb-client": x_ndb_client,
                "x-nucliadb-user": x_nucliadb_user,
                "x-forwarded-for": x_forwarded_for,
            },
            json=item.model_dump(),
        )
        if resp.status_code == 200:
            incomplete = False
        elif resp.status_code == 206:
            incomplete = True
        elif resp.status_code == 404:
            raise KnowledgeBoxNotFound()
        else:
            raise Exception(f"/find call failed: {resp.status_code} {resp.content.decode()}")

        find_results = KnowledgeboxFindResults.model_validate(resp.json())

    return find_results, incomplete
