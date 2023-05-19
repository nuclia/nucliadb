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

import pytest

from nucliadb_telemetry import context


@pytest.mark.asyncio
async def test_logger_with_context(caplog):
    context_lvl_1 = {}
    context_lvl_1_after = {}
    context_lvl_2 = {}
    context_lvl_2_after = {}
    context_lvl_3 = {}

    async def task3():
        context.add_context({"task3": "value", "foo": "daz"})
        context_lvl_3.update(context.get_context())

    async def task2():
        context.add_context({"task2": "value", "foo": "baz"})
        context_lvl_2.update(context.get_context())
        await asyncio.create_task(task3())
        context_lvl_2_after.update(context.get_context())

    async def task1():
        context.add_context({"task1": "value", "foo": "bar"})
        context_lvl_1.update(context.get_context())
        await asyncio.create_task(task2())
        context_lvl_1_after.update(context.get_context())

    await asyncio.create_task(task1())

    assert context_lvl_1 == {
        "task1": "value",
        "foo": "bar",
    }
    assert context_lvl_1 == context_lvl_1_after

    assert context_lvl_2 == {
        "task1": "value",
        "task2": "value",
        "foo": "baz",
    }
    assert context_lvl_2 == context_lvl_2_after

    assert context_lvl_3 == {
        "task1": "value",
        "task2": "value",
        "task3": "value",
        "foo": "daz",
    }
