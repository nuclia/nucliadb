import asyncio

from nucliadb_utils.fastapi.run import serve_metrics

from . import migrator
from .context import ExecutionContext
from .settings import Settings


async def run():
    settings = Settings()
    context = ExecutionContext(settings)
    await context.initialize()
    metrics_server = await serve_metrics()
    try:
        await migrator.run(context)
    finally:
        await context.finalize()
        await metrics_server.shutdown()


def main():
    asyncio.run(run())
