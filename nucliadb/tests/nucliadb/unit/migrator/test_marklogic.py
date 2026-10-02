from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.migrator import marklogic


@pytest.mark.asyncio
@pytest.mark.parametrize("already_applied", [False, True])
async def test_schema_runner_bootstraps_before_own_system_ledger(already_applied):
    driver = MarkLogicDriver("http://unused", "admin", "admin")
    events = []
    transaction = SimpleNamespace(
        get=AsyncMock(return_value=b"applied" if already_applied else None),
        set=AsyncMock(),
        commit=AsyncMock(),
    )

    async def bootstrap():
        events.append("bootstrap")

    @asynccontextmanager
    async def read_system():
        assert events == ["bootstrap"]
        events.append("read")
        yield transaction

    @asynccontextmanager
    async def write_system():
        assert events == ["bootstrap", "read", "migrate"]
        events.append("write")
        yield transaction

    async def migrate(own_driver):
        assert own_driver is driver
        events.append("migrate")

    migration = SimpleNamespace(__name__="schema_test", migrate=AsyncMock(side_effect=migrate))
    with (
        patch.object(driver, "ensure_system_database", AsyncMock(side_effect=bootstrap)) as ensure,
        patch.object(driver, "ro_transaction", side_effect=read_system) as read,
        patch.object(driver, "rw_transaction", side_effect=write_system) as write,
        patch.object(marklogic, "get_marklogic_migrations", return_value=[(1, migration)]),
        patch("nucliadb.common.maindb.utils.get_driver", side_effect=AssertionError("global driver")),
    ):
        await marklogic.run_marklogic_schema_migrations(driver)
    ensure.assert_awaited_once_with()
    read.assert_called_once_with()
    transaction.get.assert_awaited_once_with(f"{marklogic.LEDGER_PREFIX}1")
    if already_applied:
        assert events == ["bootstrap", "read"]
        write.assert_not_called()
        migration.migrate.assert_not_awaited()
        transaction.set.assert_not_awaited()
        transaction.commit.assert_not_awaited()
    else:
        assert events == ["bootstrap", "read", "migrate", "write"]
        migration.migrate.assert_awaited_once_with(driver)
        write.assert_called_once_with()
        transaction.set.assert_awaited_once_with(f"{marklogic.LEDGER_PREFIX}1", b"schema_test")
        transaction.commit.assert_awaited_once_with()
