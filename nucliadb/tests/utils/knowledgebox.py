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
from unittest.mock import AsyncMock, patch

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.tests.vectors import V1
from nucliadb_protos import utils_pb2
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_utils.storages.storage import Storage


async def create_knowledgebox(
    driver: Driver,
    *,
    kbid: str | None = None,
    slug: str | None = None,
    storage: Storage | None = None,
) -> str:
    """Create a KB through the ORM so its maindb database is fully provisioned.

    Blob storage is mocked unless a `storage` is given.
    """
    kbid = kbid or KnowledgeBox.new_unique_kbid()
    model = SemanticModelMetadata(
        similarity_function=utils_pb2.VectorSimilarity.COSINE, vector_dimension=len(V1)
    )
    with patch(
        "nucliadb.ingest.orm.knowledgebox.get_storage",
        AsyncMock(return_value=storage or AsyncMock()),
    ):
        await KnowledgeBox.create(
            driver,
            kbid=kbid,
            slug=slug or f"slug-{kbid}",
            semantic_models={"my-semantic-model": model},
        )
    return kbid
