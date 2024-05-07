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
from typing import Callable

import pytest
from httpx import AsyncClient

from nucliadb.ingest.orm.resource import Resource
from nucliadb.reader.api.v1.router import RESOURCE_PREFIX
from nucliadb_models.resource import NucliaDBRoles
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
@pytest.mark.asyncio
async def test_get_resource_all(
    reader_api: Callable[..., AsyncClient],
    test_resource: Resource,
    asyncbenchmark: AsyncBenchmarkFixture,
) -> None:
    rsc = test_resource
    kbid = rsc.kb.kbid
    rid = rsc.uuid

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await asyncbenchmark(
            client.get,
            f"/v1/kb/{kbid}/{RESOURCE_PREFIX}/{rid}",
            params={
                "show": ["basic", "origin", "relations", "values", "extracted"],
                "field_type": [
                    "text",
                    "link",
                    "file",
                    "layout",
                    "keywordset",
                    "datetime",
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
