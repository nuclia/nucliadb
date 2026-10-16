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
