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
from typing import AsyncIterable, Optional

import pytest
from httpx import AsyncClient

from nucliadb.export_import.utils import get_processor_bm, get_writer_bm
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message


@dataclass
class KbSpecs:
    kbid: str
    default_vector_dimension: Optional[int]
    vectorset_id: str
    vectorset_dimension: int


@pytest.fixture(scope="function")
async def kb_with_vectorset(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
) -> AsyncIterable[KbSpecs]:
    # Now standalone_knowledgeboxes in standalone are already created with a single vectorset.
    # By default it's the multilingual one (see mock predict implementation).
    kbid = standalone_knowledgebox
    vectorset_id = "multilingual"
    vectorset_dimension = 512
    await inject_broker_message_with_vectorset_data(
        nucliadb_ingest_grpc,
        kbid,
        vectorset_id,
        vectorset_dimension=vectorset_dimension,
    )
    yield KbSpecs(
        kbid=kbid,
        default_vector_dimension=None,
        vectorset_id=vectorset_id,
        vectorset_dimension=vectorset_dimension,
    )


async def inject_broker_message_with_vectorset_data(
    nucliadb_ingest_grpc: WriterStub,
    kbid: str,
    vectorset_id: str,
    *,
    default_vector_dimension: Optional[int] = None,
    vectorset_dimension: int,
):
    from tests.ingest.integration.ingest.test_vectorsets import (
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
    bm_writer = get_writer_bm(bm)
    await inject_message(nucliadb_ingest_grpc, bm_writer)
    bm_processor = get_processor_bm(bm)
    await inject_message(nucliadb_ingest_grpc, bm_processor)
