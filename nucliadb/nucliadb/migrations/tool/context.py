import contextlib
from typing import AsyncIterator

from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb_utils.cache import locking

from .datamanager import MigrationsDataManager
from .settings import Settings


class ExecutionContext:
    data_manager: MigrationsDataManager
    dist_lock_manager: locking.RedisDistributedLockManager
    kv_driver: Driver

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def initialize(self) -> None:
        self.kv_driver = await setup_driver()
        self.data_manager = MigrationsDataManager(self.kv_driver)
        if self.settings.redis_url is not None:
            self.dist_lock_manager = locking.RedisDistributedLockManager(
                self.settings.redis_url
            )

    async def finalize(self) -> None:
        if self.settings.redis_url is not None:
            await self.dist_lock_manager.close()
        await teardown_driver()

    @contextlib.asynccontextmanager
    async def maybe_distributed_lock(self, name: str) -> AsyncIterator[None]:
        """
        For on prem installs, redis may not be available to use for distributed locks.
        """
        if self.settings.redis_url is None:
            yield
        else:
            async with self.dist_lock_manager.lock(name):
                yield
