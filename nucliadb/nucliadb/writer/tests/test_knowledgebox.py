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
import pytest

from nucliadb.models.resource import NucliaDBRoles
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX


@pytest.mark.asyncio
async def test_knowledgebox_lifecycle(writer_api):
    async with writer_api(roles=[NucliaDBRoles.MANAGER]) as client:
        resp = await client.post(
            f"/{KBS_PREFIX}",
            json={
                "slug": "kbid1",
                "title": "My Knowledge Box",
                "description": "My lovely knowledgebox",
                "enabled_filters": ["filter1", "filter2"],
                "enabled_insights": ["insight1", "insight2"],
                "disable_vectors": True,
            },
        )
        assert resp.status_code == 201
        data = resp.json()
        assert data["slug"] == "kbid1"
        kbid = data["uuid"]

        resp = await client.patch(
            f"/{KB_PREFIX}/{kbid}",
            json={
                "slug": "kbid2",
                "description": "My lovely knowledgebox2",
                "disable_vectors": True,
            },
        )
        assert resp.status_code == 200
