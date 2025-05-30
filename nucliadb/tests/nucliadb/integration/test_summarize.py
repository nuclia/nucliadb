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


@pytest.mark.deploy_modes("standalone")
async def test_summarize(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resource_uuids = []
    resource_slugs = []

    # Create some resources
    for i in range(20):
        slug = f"resource-{i}"
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": f"Resource {i}",
                "slug": slug,
                "texts": {"text": {"body": f"My extracted text {i}"}},
            },
        )
        assert resp.status_code == 201
        resource_slugs.append(slug)
        resource_uuids.append(resp.json()["uuid"])

    # Uuids and slugs can be used interchangeably
    resources = resource_uuids[0:9] + resource_slugs[10:]

    # Summarize all of them
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/summarize", json={"resources": resources + ["non-existent"]}
    )
    assert resp.status_code == 200, resp.text

    content = resp.json()
    response = SummarizedResponse.model_validate(content)
    assert set(response.resources.keys()) == set(resources)


@pytest.mark.deploy_modes("standalone")
async def test_summarize_unexisting_kb(
    nucliadb_reader: AsyncClient,
):
    resp = await nucliadb_reader.post(f"/kb/foobar/summarize", json={"resources": ["1", "2", "3"]})
    assert resp.status_code == 404
