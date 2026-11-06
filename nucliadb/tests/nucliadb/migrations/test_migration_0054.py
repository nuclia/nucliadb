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

import uuid
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, Mock

import pytest
from pytest_mock import MockerFixture

from nucliadb.common import datamanagers
from nucliadb.migrator.models import Migration
from nucliadb_models.configuration import (
    AskConfig,
    AskSearchConfiguration,
    FindConfig,
    FindSearchConfiguration,
    SearchConfiguration,
)
from tests.nucliadb.migrations import get_migration

migration: Migration = get_migration(54)


@pytest.fixture
def transaction(mocker: MockerFixture) -> AsyncMock:
    data: dict[str, bytes] = {}

    async def keys(prefix: str) -> AsyncIterator[str]:
        for key in data:
            if key.startswith(prefix):
                yield key

    txn = AsyncMock()
    txn.keys = Mock(side_effect=keys)
    txn.set.side_effect = data.__setitem__
    txn.get.side_effect = lambda key, **kwargs: data.get(key)
    txn.batch_get.side_effect = lambda keys: [data.get(key) for key in keys]
    for mode in ("ro", "rw"):
        context = AsyncMock()
        context.__aenter__.return_value = txn
        mocker.patch.object(datamanagers, f"with_{mode}_transaction", return_value=context)
    return txn


@pytest.mark.parametrize("kind", ["find", "ask"])
async def test_migration_0054(transaction: AsyncMock, kind: str) -> None:
    kbid = str(uuid.uuid4())
    config: SearchConfiguration
    if kind == "find":
        config = FindSearchConfiguration(
            kind="find", config=FindConfig(query="preserved query", generative_model="chatgpt-o3-mini")
        )
    else:
        config = AskSearchConfiguration(
            kind="ask", config=AskConfig(query="preserved query", generative_model="chatgpt-o3-mini")
        )
    await datamanagers.search_configurations.set(transaction, kbid=kbid, name=kind, config=config)
    transaction.set.reset_mock()

    await migration.module.migrate_kb(Mock(), kbid)

    migrated = await datamanagers.search_configurations.get(transaction, kbid=kbid, name=kind)
    assert migrated is not None
    expected = config.model_dump()
    expected["config"]["generative_model"] = "chatgpt-5.6-sol"
    assert migrated.model_dump() == expected
    transaction.set.assert_awaited_once()
    transaction.commit.assert_awaited_once()

    transaction.set.reset_mock()
    transaction.commit.reset_mock()
    await migration.module.migrate_kb(Mock(), kbid)

    repeated = await datamanagers.search_configurations.get(transaction, kbid=kbid, name=kind)
    assert repeated == migrated
    transaction.set.assert_not_awaited()
    transaction.commit.assert_not_awaited()


@pytest.mark.parametrize("kind", ["find", "ask"])
@pytest.mark.parametrize(
    "model",
    [
        "chatgpt4o",
        "chatgpt4o-mini",
        "chatgpt-azure-4o",
        "chatgpt-azure-4o-mini",
        "chatgpt-azure-o3-mini",
        "chatgpt-5.6-sol",
        "chatgpt-o3-mini/550e8400-e29b-41d4-a716-446655440000",
        None,
    ],
)
async def test_migration_0054_preserves_other_models(
    transaction: AsyncMock, kind: str, model: str | None
) -> None:
    kbid = str(uuid.uuid4())
    config: SearchConfiguration
    if kind == "find":
        config = FindSearchConfiguration(kind="find", config=FindConfig(generative_model=model))
    else:
        config = AskSearchConfiguration(kind="ask", config=AskConfig(generative_model=model))
    await datamanagers.search_configurations.set(transaction, kbid=kbid, name=kind, config=config)
    transaction.set.reset_mock()

    await migration.module.migrate_kb(Mock(), kbid)

    preserved = await datamanagers.search_configurations.get(transaction, kbid=kbid, name=kind)
    assert preserved == config
    transaction.set.assert_not_awaited()
    transaction.commit.assert_not_awaited()


async def test_migration_0054_without_search_configurations(transaction: AsyncMock) -> None:
    await migration.module.migrate_kb(Mock(), str(uuid.uuid4()))

    transaction.set.assert_not_awaited()
    transaction.commit.assert_not_awaited()
