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
