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
import time

import pytest
from httpx import AsyncClient

from nucliadb.ingest.orm.resource import Resource
from nucliadb.reader.api.v1.router import KB_PREFIX, RESOURCE_PREFIX
from nucliadb_utils.tests.asyncbenchmark import AsyncBenchmarkFixture


@pytest.mark.benchmark(
    group="resource",
    min_time=0.1,
    max_time=0.5,
    min_rounds=5,
    timer=time.time,
    disable_gc=True,
    warmup=False,
)
@pytest.mark.deploy_modes("component")
async def test_get_resource_all(
    nucliadb_reader: AsyncClient,
    full_resource: Resource,
    asyncbenchmark: AsyncBenchmarkFixture,
) -> None:
    resource = full_resource
    kbid = resource.kbid
    rid = resource.uuid

    resp = await asyncbenchmark(
        nucliadb_reader.get,
        f"/{KB_PREFIX}/{kbid}/{RESOURCE_PREFIX}/{rid}",
        params={
            "show": ["basic", "origin", "relations", "values", "extracted"],
            "field_type": [
                "text",
                "link",
                "file",
                "conversation",
            ],
            "extracted": [
                "metadata",
                "vectors",
                "large_metadata",
                "text",
                "link",
                "file",
            ],
        },
    )
    assert resp.status_code == 200
