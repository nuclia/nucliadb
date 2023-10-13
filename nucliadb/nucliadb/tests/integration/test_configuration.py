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

from typing import Dict

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_kb_configuration(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox: str,
):
    kbid = knowledgebox

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/configuration",
        json={
            "semantic_model": "test1",
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/configuration")
    assert resp.status_code == 200
    body: Dict[str, str] = resp.json()
    assert len(body) == 1
    assert body.get("semantic_model") == "test1"

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/configuration",
        json={
            "generative_model": "test1",
            "ner_model": "test2",
            "anonymization_model": "test2",
            "visual_labeling": "test2",
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/configuration")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 5

    resp = await nucliadb_writer.delete(f"/kb/{kbid}/configuration")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{kbid}/configuration")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 0
