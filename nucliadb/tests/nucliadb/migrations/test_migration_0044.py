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
from unittest.mock import Mock

from nucliadb.common import datamanagers
from nucliadb.common.maindb.driver import Driver
from nucliadb.migrator.models import Migration
from nucliadb_models.configuration import (
    AskConfig,
    AskSearchConfiguration,
    FindConfig,
    FindSearchConfiguration,
)
from tests.nucliadb.migrations import get_migration

migration: Migration = get_migration(44)


async def test_migration_0044(maindb_driver: Driver):
    execution_context = Mock()
    execution_context.kv_driver = maindb_driver
    storage = Mock()
    execution_context.blob_storage = storage

    kbid = str(uuid.uuid4())

    # Create search configurations using deprecated model names
    async with maindb_driver.rw_transaction() as txn:
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="claude-3-5-fast",
            config=FindSearchConfiguration(
                kind="find", config=FindConfig(generative_model="claude-3-5-fast")
            ),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="claude-3",
            config=AskSearchConfiguration(kind="ask", config=AskConfig(generative_model="claude-3")),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="azure-mistral-large-2",
            config=FindSearchConfiguration(
                kind="find", config=FindConfig(generative_model="azure-mistral-large-2")
            ),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="llama",
            config=AskSearchConfiguration(
                kind="ask",
                config=AskConfig(generative_model="llama-3.2-90b-vision-instruct-maas"),
            ),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="gemini-3-pro",
            config=FindSearchConfiguration(
                kind="find", config=FindConfig(generative_model="gemini-3-pro")
            ),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="aws-claude-3-7-sonnet",
            config=FindSearchConfiguration(
                kind="find", config=FindConfig(generative_model="aws-claude-3-7-sonnet")
            ),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="gcp-claude-3-7-sonnet",
            config=AskSearchConfiguration(
                kind="ask", config=AskConfig(generative_model="gcp-claude-3-7-sonnet")
            ),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="claude-4-opus",
            config=FindSearchConfiguration(
                kind="find", config=FindConfig(generative_model="claude-4-opus")
            ),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="aws-claude-4-opus",
            config=AskSearchConfiguration(
                kind="ask", config=AskConfig(generative_model="aws-claude-4-opus")
            ),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="gemini-2.0-flash",
            config=FindSearchConfiguration(
                kind="find", config=FindConfig(generative_model="gemini-2.0-flash")
            ),
        )
        await datamanagers.search_configurations.set(
            txn,
            kbid=kbid,
            name="gemini-2.0-flash-lite",
            config=AskSearchConfiguration(
                kind="ask", config=AskConfig(generative_model="gemini-2.0-flash-lite")
            ),
        )
        await txn.commit()

    await migration.module.migrate_kb(execution_context, kbid)

    # Make sure all generative_models have been replaced with the correct targets
    async with maindb_driver.ro_transaction() as txn:
        expected = {
            "claude-3-5-fast": "claude-4-5-sonnet",
            # claude-3 resolves directly to claude-4-7-opus (bypassing intermediate claude-4-opus)
            "claude-3": "claude-4-7-opus",
            "azure-mistral-large-2": "chatgpt-azure-5",
            "llama": "llama-4-scout-17b-16e-instruct-maas",
            "gemini-3-pro": "gemini-3.1-pro",
            "aws-claude-3-7-sonnet": "aws-claude-4-6-sonnet",
            "gcp-claude-3-7-sonnet": "gcp-claude-4-6-sonnet",
            "claude-4-opus": "claude-4-7-opus",
            "aws-claude-4-opus": "aws-claude-4-6-opus",
            "gemini-2.0-flash": "gemini-2.5-flash",
            "gemini-2.0-flash-lite": "gemini-2.5-flash-lite",
        }
        for name, expected_model in expected.items():
            config = await datamanagers.search_configurations.get(txn, kbid=kbid, name=name)
            assert config is not None, f"Config '{name}' not found"
            assert config.config.generative_model == expected_model, (  # type: ignore[attr-defined]
                f"Config '{name}': expected '{expected_model}', got '{config.config.generative_model}'"  # type: ignore[attr-defined]
            )
