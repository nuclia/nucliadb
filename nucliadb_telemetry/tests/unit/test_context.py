# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import asyncio

from nucliadb_telemetry import context


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
