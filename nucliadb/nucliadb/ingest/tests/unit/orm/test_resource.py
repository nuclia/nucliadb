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
from unittest.mock import AsyncMock

import pytest
from nucliadb_protos.resources_pb2 import (
    Basic,
    CloudFile,
    FileExtractedData,
    PagePositions,
)

from nucliadb.ingest.orm.resource import (
    get_file_page_positions,
    maybe_update_basic_summary,
    maybe_update_basic_thumbnail,
)

pytestmark = pytest.mark.asyncio


async def test_get_file_page_positions():
    extracted_data = FileExtractedData()
    extracted_data.file_pages_previews.positions.extend(
        [PagePositions(start=0, end=10), PagePositions(start=11, end=20)]
    )
    file_field = AsyncMock(
        get_file_extracted_data=AsyncMock(return_value=extracted_data)
    )
    assert await get_file_page_positions(file_field) == {0: (0, 10), 1: (11, 20)}


@pytest.mark.parametrize(
    "basic,summary,updated",
    [
        (Basic(), "new_summary", True),
        (Basic(summary="summary"), "new_summary", False),
        (Basic(summary="summary"), "", False),
    ],
)
def test_maybe_update_basic_summary(basic, summary, updated):
    assert maybe_update_basic_summary(basic, summary) == updated
    if updated:
        assert basic.summary == summary
    else:
        assert basic.summary != summary


@pytest.mark.parametrize(
    "basic,thumbnail,updated",
    [
        (Basic(), CloudFile(uri="new_thumbnail_url"), True),
        (
            Basic(thumbnail="old_thumbnail_url"),
            CloudFile(uri="new_thumbnail_url"),
            False,
        ),
        (Basic(thumbnail="old_thumbnail_url"), None, False),
    ],
)
def test_maybe_update_basic_thumbnail(basic, thumbnail, updated):
    assert maybe_update_basic_thumbnail(basic, thumbnail) == updated
    if updated:
        assert basic.thumbnail == thumbnail.uri
    else:
        assert basic.thumbnail == "old_thumbnail_url"
