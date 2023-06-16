import asyncio

from . import migrator
from .context import ExecutionContext
from .settings import Settings


async def run():
    settings = Settings()
    context = ExecutionContext(settings)
    await context.initialize()
    try:
        await migrator.run(context)
    finally:
        await context.finalize()


def main():
    asyncio.run(run())
