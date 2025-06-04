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

import asyncio
from unittest.mock import AsyncMock, Mock

from nucliadb.ingest.fields.file import File
from nucliadb_protos.writer_pb2 import FieldComputedMetadata


async def test_field_caches_field_metadata_properly():
    computed_metadata = FieldComputedMetadata()
    storage = Mock()
    storage.download_pb = AsyncMock(return_value=computed_metadata)
    resource = Mock(storage=storage)
    ffield = File(
        "id",
        resource=resource,
    )

    assert await ffield.get_field_metadata() is computed_metadata
    assert await ffield.get_field_metadata() is computed_metadata

    assert storage.download_pb.call_count == 1

    fm = await ffield.get_field_metadata(force=True)
    assert fm is computed_metadata

    assert storage.download_pb.call_count == 2

    storage.download_pb.reset_mock()

    ffield.computed_metadata = None

    tasks = []
    for _ in range(10):
        tasks.append(asyncio.create_task(ffield.get_field_metadata()))
    results = await asyncio.gather(*tasks)
    assert all(res is computed_metadata for res in results)

    assert storage.download_pb.call_count == 1
