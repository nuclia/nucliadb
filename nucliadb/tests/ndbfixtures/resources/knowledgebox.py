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
from typing import AsyncIterator

import pytest
from httpx import AsyncClient

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.tests.vectors import V1
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX
from nucliadb_protos import utils_pb2 as upb
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_utils.storages.storage import Storage


@pytest.fixture(scope="function")
async def knowledgebox(
    storage: Storage,
    maindb_driver: Driver,
    shard_manager: KBShardManager,
) -> AsyncIterator[str]:
    """Knowledgebox created through the ORM. This is what ingest gRPC ends up
    calling when backend creates a hosted KB and what the standalone API ends up
    calling for onprem KBs.

    """
    kbid = KnowledgeBox.new_unique_kbid()
    kbslug = "slug-" + str(uuid.uuid4())
    model = SemanticModelMetadata(
        similarity_function=upb.VectorSimilarity.COSINE, vector_dimension=len(V1)
    )
    await KnowledgeBox.create(
        maindb_driver, kbid=kbid, slug=kbslug, semantic_models={"my-semantic-model": model}
    )

    yield kbid

    await KnowledgeBox.delete(maindb_driver, kbid)
    # await KnowledgeBox.purge(maindb_driver, kbid)


@pytest.fixture(scope="function")
async def standalone_knowledgebox(nucliadb_writer_manager: AsyncClient) -> AsyncIterator[str]:
    """Knowledgebox created through the /kbs endpoint, only accessible for
    onprem (standalone) deployments.

    This fixture requires the test using it to be decorated with a
    @pytest.mark.deploy_modes(...)

    """
    resp = await nucliadb_writer_manager.post(
        f"/{KBS_PREFIX}",
        json={
            "title": "Standalone test KB",
            "slug": "knowledgebox",
        },
    )
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    yield kbid

    resp = await nucliadb_writer_manager.delete(f"/{KB_PREFIX}/{kbid}")
    assert resp.status_code == 200
