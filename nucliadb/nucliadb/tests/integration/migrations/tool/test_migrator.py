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

from nucliadb.migrations.tool import migrator
from nucliadb.migrations.tool.context import ExecutionContext
from nucliadb.migrations.tool.settings import Settings
from nucliadb.migrations.tool.utils import get_latest_version

pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def execution_context(redis_config):
    settings = Settings(redis_url=redis_config)
    context = ExecutionContext(settings)
    await context.initialize()
    yield context
    try:
        await migrator.run(context)
    finally:
        await context.finalize()


async def test_migrate_kb(execution_context: ExecutionContext, knowledgebox):
    # this will test run all avilable migrations
    await execution_context.data_manager.update_kb_info(
        kbid=knowledgebox, current_version=-1
    )
    await migrator.run(execution_context)

    kb_info = await execution_context.data_manager.get_kb_info(kbid=knowledgebox)
    assert kb_info.current_version == get_latest_version()  # type: ignore
    global_info = await execution_context.data_manager.get_global_info()
    assert global_info.current_version == get_latest_version()
