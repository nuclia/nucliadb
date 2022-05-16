import asyncio

import pytest
from httpx import AsyncClient

from nucliadb_telemetry.tests.telemetry import JAEGGER_MESSAGES, Greeter


@pytest.mark.asyncio
async def test_telemetry(http_service: AsyncClient, greeter: Greeter):
    resp = await http_service.get(
        "http://test/",
        headers={
            "x-b3-traceid": "f13dc5318bf3bef64a0a5ea607db93a1",
            "x-b3-spanid": "bfc2225c60b39d97",
            "x-b3-sampled": "1",
        },
    )
    assert resp.status_code == 200
    for i in range(10):
        if len(greeter.messages) == 0:
            await asyncio.sleep(1)
    assert (
        greeter.messages[0].headers["x-b3-traceid"]
        == "f13dc5318bf3bef64a0a5ea607db93a1"
    )

    for i in range(10):
        if len(JAEGGER_MESSAGES) == 0:
            await asyncio.sleep(2)
    assert len(JAEGGER_MESSAGES) > 0
