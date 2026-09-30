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
import asyncio
import json
import logging
from collections.abc import AsyncIterator

import pytest

from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.marklogic import MarkLogicDriver, MarkLogicTransaction
from nucliadb_utils.utilities import Utility
from tests.ndbfixtures.utils import global_utility

logger = logging.getLogger("nucliadb.fixtures:maindb")


@pytest.fixture(scope="function")
async def maindb_driver(marklogic_maindb_driver) -> AsyncIterator[Driver]:
    driver: Driver = marklogic_maindb_driver

    with global_utility(Utility.MAINDB_DRIVER, driver):
        yield driver

    try:
        await cleanup_maindb(driver)
    except Exception:
        logger.exception("Could not cleanup maindb on test teardown")


async def cleanup_maindb(driver: Driver):
    if not driver.initialized:
        return
    async with driver.rw_transaction() as txn:
        await txn.delete_by_prefix("/")
        await txn.commit()
    async with driver.ro_transaction() as txn:
        assert await txn.count("/") == 0

    if isinstance(driver, MarkLogicDriver):
        await cleanup_marklogic_collections(driver)


async def cleanup_marklogic_collections(driver: MarkLogicDriver) -> None:
    for collection in MarkLogicCollections.all():
        async with driver.rw_transaction() as txn:
            assert isinstance(txn, MarkLogicTransaction)
            assert txn.transaction is not None
            uris = (
                await asyncio.to_thread(
                    driver.client.eval,
                    javascript=(
                        f"cts.uris('', ['document'], cts.collectionQuery({json.dumps(collection)}))"
                    ),
                    tx=txn.transaction,
                    params={"database": driver.database},
                )
                or []
            )
            for uri in uris:
                response = await asyncio.to_thread(
                    driver.client.delete,
                    "/v1/documents",
                    params={
                        "database": driver.database,
                        "uri": str(uri),
                        "txid": txn.transaction.id,
                    },
                )
                response.raise_for_status()
            await txn.commit()
