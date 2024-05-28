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

import pytest

from nucliadb.ingest.consumer import utils

pytestmark = pytest.mark.asyncio


async def test_delay_task_handler():
    dth = utils.DelayedTaskHandler(0.05)
    await dth.initialize()

    counter = 0

    async def handler():
        await asyncio.sleep(0.1)
        nonlocal counter
        counter += 1

    dth.schedule("key1", handler)
    dth.schedule("key1", handler)
    dth.schedule("key1", handler)
    dth.schedule("key2", handler)
    dth.schedule("key3", handler)
    dth.schedule("key4", handler)

    # all should be scheduled and duplicates ignored
    assert len(dth.to_process) == 4

    await asyncio.sleep(0.06)
    # they should all be running now
    assert len(dth.outstanding_tasks) == 4

    # schedule a couple more
    dth.schedule("key1", handler)  # duplicate key, should get rescheduled at end
    dth.schedule("key5", handler)
    dth.schedule("key6", handler)

    await asyncio.sleep(0.1)
    # original set should be finished now
    assert counter == 4

    # finish everything now
    await dth.finalize()

    assert counter == 7
