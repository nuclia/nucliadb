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

import pytest

from nucliadb.migrator import migrator
from nucliadb.migrator.context import ExecutionContext
from nucliadb.migrator.settings import Settings

pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def execution_context(natsd, storage, redis_config, nucliadb):
    settings = Settings(redis_url=redis_config)
    context = ExecutionContext(settings)
    await context.initialize()
    yield context
    try:
        await migrator.run(context)
    finally:
        await context.finalize()


@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_migrate_kb(execution_context: ExecutionContext, knowledgebox):
    # this will test run all available migrations
    await execution_context.data_manager.update_kb_info(
        kbid=knowledgebox, current_version=-1
    )
    await execution_context.data_manager.update_global_info(current_version=0)

    kb_info = await execution_context.data_manager.get_kb_info(kbid=knowledgebox)
    assert kb_info is not None
    assert kb_info.current_version == -1
    global_info = await execution_context.data_manager.get_global_info()
    assert global_info.current_version == 0

    # only run first noop migration
    # other tests can be so slow and cumbersome to maintain
    await migrator.run(execution_context, target_version=1)

    kb_info = await execution_context.data_manager.get_kb_info(kbid=knowledgebox)
    assert kb_info is not None
    assert kb_info.current_version == 1
    global_info = await execution_context.data_manager.get_global_info()
    assert global_info.current_version == 1


@pytest.fixture(scope="function")
async def two_knowledgeboxes(nucliadb_manager):
    kbs = []
    for _ in range(2):
        resp = await nucliadb_manager.post("/kbs", json={"slug": uuid.uuid4().hex})
        assert resp.status_code == 201
        kbs.append(resp.json().get("uuid"))

    yield kbs

    for kb in kbs:
        resp = await nucliadb_manager.delete(f"/kb/{kb}")
        assert resp.status_code == 200


async def test_run_all_kb_migrations(
    execution_context: ExecutionContext, two_knowledgeboxes
):
    # Set migration version to -1 for all knowledgeboxes
    for kbid in two_knowledgeboxes:
        await execution_context.data_manager.update_kb_info(
            kbid=kbid, current_version=-1
        )
        await execution_context.data_manager.update_global_info(current_version=0)

        kb_info = await execution_context.data_manager.get_kb_info(kbid=kbid)
        assert kb_info is not None
        assert kb_info.current_version == -1
        global_info = await execution_context.data_manager.get_global_info()
        assert global_info.current_version == 0

    # only run first noop migration
    # other tests can be so slow and cumbersome to maintain
    await migrator.run(execution_context, target_version=1)

    # Assert that all knowledgeboxes are now at version 1
    for kbid in two_knowledgeboxes:
        kb_info = await execution_context.data_manager.get_kb_info(kbid=kbid)
        assert kb_info is not None
        assert kb_info.current_version == 1
        global_info = await execution_context.data_manager.get_global_info()
        assert global_info.current_version == 1


async def test_run_kb_rollovers(
    execution_context: ExecutionContext, two_knowledgeboxes
):
    # Set migration version to -1 for all knowledgeboxes
    for kbid in two_knowledgeboxes:
        await execution_context.data_manager.add_kb_rollover(kbid=kbid)

    assert set(await execution_context.data_manager.get_kbs_to_rollover()) == set(
        two_knowledgeboxes
    )

    await migrator.run(execution_context, target_version=0)

    assert len(await execution_context.data_manager.get_kbs_to_rollover()) == 0
