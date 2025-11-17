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
from httpx import AsyncClient

from nucliadb.search.api.v1.router import KB_PREFIX


@pytest.mark.deploy_modes("standalone")
async def test_labelsets_service(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
) -> None:
    kbid = standalone_knowledgebox

    payload = {
        "title": "labelset1",
        "labels": [{"title": "Label 1", "related": "related 1", "text": "My Text"}],
    }
    resp = await nucliadb_writer.post(f"/{KB_PREFIX}/{kbid}/labelset/labelset1", json=payload)
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{kbid}/labelsets")
    assert len(resp.json()["labelsets"]) == 1

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{kbid}/labelset/labelset1")
    assert resp.status_code == 200

    resp = await nucliadb_writer.delete(f"/{KB_PREFIX}/{kbid}/labelset/labelset1")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{kbid}/labelsets")
    assert len(resp.json()["labelsets"]) == 0


@pytest.mark.deploy_modes("standalone")
async def test_notifications_service(
    nucliadb_reader: AsyncClient,
):
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/foobar/notifications")
    assert resp.status_code == 404
