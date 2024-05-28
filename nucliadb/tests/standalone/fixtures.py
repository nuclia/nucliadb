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
import uuid

import pytest

from nucliadb.search.api.v1.router import KB_PREFIX, KBS_PREFIX


@pytest.fixture(scope="function")
async def knowledgebox_one(nucliadb_manager):
    kbslug = str(uuid.uuid4())
    data = {"slug": kbslug}
    resp = await nucliadb_manager.post(f"/{KBS_PREFIX}", json=data)
    assert resp.status_code == 201
    kbid = resp.json()["uuid"]

    yield kbid

    resp = await nucliadb_manager.delete(f"/{KB_PREFIX}/{kbid}")
    assert resp.status_code == 200
