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

import random
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import (
    KB_VECTORSET_TO_DELETE,
    KnowledgeBox,
)
from nucliadb.purge import purge_kb_vectorsets
from nucliadb_protos import resources_pb2, utils_pb2, writer_pb2
from nucliadb_protos.knowledgebox_pb2 import VectorSetConfig, VectorSetPurge
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.storages.storage import Storage
from tests.ingest.fixtures import make_extracted_text
from tests.utils import inject_message


@pytest.mark.deploy_modes("standalone")
async def test_purge_vectorsets__kb_with_vectorsets(
    maindb_driver: Driver,
    storage: Storage,
    nucliadb_ingest_grpc: WriterStub,
    knowledgebox_with_vectorsets: str,
):
    kbid = knowledgebox_with_vectorsets
    vectorset_id = "my-semantic-model-A"

    resource_count = 5
    for i in range(resource_count):
        bm = await create_broker_message_with_vectorset(kbid, maindb_driver)
        await inject_message(nucliadb_ingest_grpc, bm)

    with patch.object(
        storage, "delete_upload", new=AsyncMock(side_effect=storage.delete_upload)
    ) as mock:
        async with maindb_driver.transaction(read_only=False) as txn:
            kb_obj = KnowledgeBox(txn, storage, kbid)
            await kb_obj.delete_vectorset(vectorset_id)
            await txn.commit()

        async with maindb_driver.transaction(read_only=True) as txn:
            value = await txn.get(KB_VECTORSET_TO_DELETE.format(kbid=kbid, vectorset=vectorset_id))
            assert value is not None
            purge_payload = VectorSetPurge()
            purge_payload.ParseFromString(value)
            assert purge_payload.storage_key_kind == VectorSetConfig.StorageKeyKind.VECTORSET_PREFIX

        await purge_kb_vectorsets(maindb_driver, storage)

        async with maindb_driver.transaction(read_only=True) as txn:
            value = await txn.get(KB_VECTORSET_TO_DELETE.format(kbid=kbid, vectorset=vectorset_id))
            assert value is None

        # XXX: this should be field count for a better test
        assert mock.await_count == resource_count
        storage_key = mock.await_args.args[0]  # type: ignore
        assert storage_key.endswith(f"/{vectorset_id}/extracted_vectors")


async def create_broker_message_with_vectorset(
    kbid: str,
    driver: Driver,
):
    rid = str(uuid.uuid4())
    field_id = f"{rid}-text-field"
    bm = writer_pb2.BrokerMessage(kbid=kbid, uuid=rid, type=writer_pb2.BrokerMessage.AUTOCOMMIT)

    body = "Lorem ipsum dolor sit amet..."
    bm.texts[field_id].body = body

    bm.extracted_text.append(make_extracted_text(field_id, body))

    async with driver.transaction(read_only=True) as txn:
        async for vectorset_id, vs in datamanagers.vectorsets.iter(txn, kbid=kbid):
            # custom vectorset
            field_vectors = resources_pb2.ExtractedVectorsWrapper()
            field_vectors.field.field = field_id
            field_vectors.field.field_type = resources_pb2.FieldType.TEXT
            field_vectors.vectorset_id = vectorset_id
            for i in range(0, 100, 10):
                field_vectors.vectors.vectors.vectors.append(
                    utils_pb2.Vector(
                        start=i,
                        end=i + 10,
                        vector=[random.random()] * vs.vectorset_index_config.vector_dimension,
                    )
                )
            bm.field_vectors.append(field_vectors)

    return bm
