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

from nucliadb_models.search import SummarizedResponse


@pytest.mark.asyncio()
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_summarize(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    kbid = knowledgebox

    resource_uuids = []

    # Create some resources
    for i in range(25):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": f"Resource {i}",
                "texts": {"text": {"body": f"My extracted text {i}"}},
            },
        )
        assert resp.status_code == 201
        resource_uuids.append(resp.json()["uuid"])

    # Summarize all of them
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/summarize", json={"resources": resource_uuids}
    )
    assert resp.status_code == 200, resp.text

    content = resp.json()
    response = SummarizedResponse.parse_obj(content)
    assert set(response.resources.keys()) == set(resource_uuids)
