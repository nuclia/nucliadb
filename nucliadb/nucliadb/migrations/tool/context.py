from .settings import Settings

from .datamanager import MigrationsDataManager
from nucliadb_utils.cache import locking
from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb.common.maindb.driver import Driver


class ExecutionContext:
    data_manager = MigrationsDataManager
    dist_lock_manager = locking.RedisDistributedLockManager
    kv_driver: Driver

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    async def initialize(self) -> None:
        self.kv_driver = setup_driver()
        self.data_manager = MigrationsDataManager(self.kv_driver)
        self.dist_lock_manager = locking.RedisDistributedLockManager(
            self.settings.redis_url
        )

    async def finalize(self) -> None:
        await self.dist_lock_manager.close()
        await teardown_driver()
