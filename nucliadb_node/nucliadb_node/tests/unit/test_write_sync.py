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

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from nucliadb_node import write_sync

pytestmark = pytest.mark.asyncio


async def test_start():
    with patch("nucliadb_node.write_sync.sync_fs_index", AsyncMock()) as sync_fs_index:
        finalizer = write_sync.start()
        await finalizer()

        sync_fs_index.assert_called_once()


async def test_sync_fs_index():
    with patch("nucliadb_node.write_sync.Observer") as observer:
        task = asyncio.create_task(write_sync.sync_fs_index())
        await asyncio.sleep(0.1)

        task.cancel()

        await asyncio.sleep(0.01)

        observer().join.assert_called_once()


def test_recorder_handler(tmp_path):
    fi = tmp_path / "file.txt"
    fi.write_text("foobar")

    event = Mock(src_path=str(fi))

    rh = write_sync.RecorderHandler()

    with patch("nucliadb_node.write_sync.histogram") as histogram:
        rh.on_created(event)
        rh.on_modified(event)

        assert len(histogram.mock_calls) == 2
        histogram.observe.assert_called_with(6)
