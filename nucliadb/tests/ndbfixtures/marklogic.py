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
"""Fixture for an already-running local MarkLogic Compose service."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx
import pytest

from nucliadb.common.maindb.marklogic import MarkLogicDriver


async def _wait_until_ready(port: int) -> None:
    async with httpx.AsyncClient(
        base_url=f"http://127.0.0.1:{port}",
        auth=httpx.DigestAuth("admin", "admin"),
        timeout=5,
    ) as client:
        for _ in range(90):
            try:
                response = await client.get("/manage/v2", params={"format": "json"})
                if response.status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(1)
    raise TimeoutError("MarkLogic Compose service did not become ready on port 8002")


@pytest.fixture(scope="function")
def marklogic_maindb_settings() -> dict:
    return {
        "uri": "http://127.0.0.1",
        "username": "admin",
        "password": "admin",
        "port": 8000,
        "admin_port": 8002,
        "preallocated_partitions": 1,
        "replication": False,
    }


@pytest.fixture(scope="function")
async def marklogic_maindb_driver(marklogic_maindb_settings: dict) -> AsyncIterator[MarkLogicDriver]:
    """Connect to MarkLogic started externally by docker compose."""
    await _wait_until_ready(marklogic_maindb_settings["admin_port"])
    driver = MarkLogicDriver(**marklogic_maindb_settings)
    await driver.initialize()
    try:
        yield driver
    finally:
        await driver.finalize()
