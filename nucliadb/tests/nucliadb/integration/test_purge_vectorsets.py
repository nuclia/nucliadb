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

import pytest

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KB_VECTORSET_TO_DELETE
from nucliadb.purge import purge_kb_vectorsets
from nucliadb_protos import writer_pb2
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.storages.storage import Storage
from tests.nucliadb.knowledgeboxes.vectorsets import KbSpecs


@pytest.mark.asyncio
async def test_purge_vectorsets(
    maindb_driver: Driver,
    storage: Storage,
    nucliadb_grpc: WriterStub,
    kb_with_vectorset: KbSpecs,
):
    kbid = kb_with_vectorset.kbid
    vectorset_id = kb_with_vectorset.vectorset_id

    with patch.object(
        storage, "delete_upload", new=AsyncMock(side_effect=storage.delete_upload)
    ) as mock:
        response = await nucliadb_grpc.DelVectorSet(
            writer_pb2.DelVectorSetRequest(kbid=kbid, vectorset_id=vectorset_id)
        )  # type: ignore
        assert response.status == response.Status.OK

        async with maindb_driver.transaction(read_only=True) as txn:
            value = await txn.get(KB_VECTORSET_TO_DELETE.format(kbid=kbid, vectorset=vectorset_id))
            assert value is not None

        await purge_kb_vectorsets(maindb_driver, storage)

        async with maindb_driver.transaction(read_only=True) as txn:
            value = await txn.get(KB_VECTORSET_TO_DELETE.format(kbid=kbid, vectorset=vectorset_id))
            assert value is None

        assert mock.await_count == 1
        storage_key = mock.await_args.args[0]  # type: ignore
        assert storage_key.endswith(f"/{vectorset_id}/extracted_vectors")
