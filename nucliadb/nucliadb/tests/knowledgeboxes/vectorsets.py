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
from dataclasses import dataclass
from typing import AsyncIterable

import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.tests.utils import inject_message
from nucliadb_protos import writer_pb2


@dataclass
class KbSpecs:
    kbid: str
    default_vector_dimension: int
    vectorset_id: str
    vectorset_dimension: int


@pytest.fixture(scope="function")
async def kb_with_vectorset(
    nucliadb_manager: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox: str,
) -> AsyncIterable[KbSpecs]:
    kbid = knowledgebox
    vectorset_id = "my-vectorset"
    default_vector_dimension = 512
    vectorset_dimension = 324

    await create_vectorset(
        nucliadb_grpc, kbid, vectorset_id, vectorset_dimension=vectorset_dimension
    )
    await inject_broker_message_with_vectorset_data(
        nucliadb_grpc,
        kbid,
        vectorset_id,
        default_vector_dimension=default_vector_dimension,
        vectorset_dimension=vectorset_dimension,
    )

    yield KbSpecs(
        kbid=kbid,
        default_vector_dimension=default_vector_dimension,
        vectorset_id=vectorset_id,
        vectorset_dimension=vectorset_dimension,
    )


async def create_vectorset(
    nucliadb_grpc: WriterStub, kbid: str, vectorset_id: str, *, vectorset_dimension: int
):
    response = await nucliadb_grpc.NewVectorSet(
        writer_pb2.NewVectorSetRequest(
            kbid=kbid,
            vectorset_id=vectorset_id,
            vector_dimension=vectorset_dimension,
        )
    )  # type: ignore
    assert response.status == response.Status.OK


async def inject_broker_message_with_vectorset_data(
    nucliadb_grpc: WriterStub,
    kbid: str,
    vectorset_id: str,
    *,
    default_vector_dimension: int,
    vectorset_dimension: int,
):
    from nucliadb.ingest.tests.integration.ingest.test_vectorsets import (
        create_broker_message_with_vectorset,
    )

    bm = create_broker_message_with_vectorset(
        kbid,
        rid=uuid.uuid4().hex,
        field_id=uuid.uuid4().hex,
        vectorset_id=vectorset_id,
        default_vectorset_dimension=default_vector_dimension,
        vectorset_dimension=vectorset_dimension,
    )
    await inject_message(nucliadb_grpc, bm)
