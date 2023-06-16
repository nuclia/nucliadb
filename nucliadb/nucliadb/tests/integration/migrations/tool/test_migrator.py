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
    assert kb_info.current_version == get_latest_version()
    global_info = await execution_context.data_manager.get_global_info()
    assert global_info.current_version == get_latest_version()
