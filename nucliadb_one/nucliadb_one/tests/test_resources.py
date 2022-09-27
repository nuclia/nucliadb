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
from typing import Callable

import pytest
from httpx import AsyncClient

from nucliadb_models.resource import NucliaDBRoles
from nucliadb_search.api.v1.router import KB_PREFIX


@pytest.mark.asyncio
async def test_last_seqid_in_resource(
    nucliadb_api: Callable[..., AsyncClient], knowledgebox_one
) -> None:

    async with nucliadb_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{knowledgebox_one}/resources",
            headers={"X-SYNCHRONOUS": "True"},
            json={
                "texts": {
                    "textfield1": {"body": "Some text", "format": "PLAIN"},
                }
            }
        )
        assert resp.status_code == 201
        data = resp.json()
        rid = data["uuid"]
        seqid = data["seqid"]

    async with nucliadb_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{knowledgebox_one}/resource/{rid}")
        assert seqid == resp.json()["last_seqid"]
