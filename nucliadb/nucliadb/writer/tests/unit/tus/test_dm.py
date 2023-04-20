from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.writer import tus
from nucliadb.writer.tus.dm import (
    FileDataMangaer,
    RedisFileDataManager,
    RedisFileDataManagerFactory,
)


@pytest.fixture()
def redis():
    mock = AsyncMock()
    with patch("nucliadb.writer.tus.dm.aioredis.from_url", return_value=mock):
        yield mock


async def test_file_data_manager_factory(redis):
    factory = RedisFileDataManagerFactory("redis://localhost:6379")
    inst = factory()

    assert isinstance(inst, RedisFileDataManager)
    assert inst.redis == redis

    await factory.finalize()

    redis.close.assert_called_once_with(close_connection_pool=True)


async def test_get_file_data_manager():
    await tus.finalize()  # make sure to clear

    with patch.object(tus.writer_settings, "dm_enabled", False):
        assert isinstance(tus.get_dm(), FileDataMangaer)


async def test_get_file_data_manager_redis(redis):
    await tus.finalize()  # make sure to clear

    with patch.object(tus.writer_settings, "dm_enabled", True):
        assert isinstance(tus.get_dm(), RedisFileDataManager)
