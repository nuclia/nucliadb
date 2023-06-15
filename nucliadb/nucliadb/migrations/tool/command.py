from . import migrator
from .settings import Settings
from .context import ExecutionContext
import asyncio


async def run():
    settings = Settings()
    context = ExecutionContext(settings)
    await context.initialize()
    try:
        await migrator.run(context)
    finally:
        await context.finalize()


def main(self):
    asyncio.run(run())
