import asyncio

import pytest
from httpx import AsyncClient

from nucliadb_telemetry.settings import telemetry_settings
from nucliadb_telemetry.tests.telemetry import Greeter


@pytest.mark.asyncio
async def test_telemetry_dict(http_service: AsyncClient, greeter: Greeter):
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

    await asyncio.sleep(2)
    client = AsyncClient()
    for _ in range(10):
        resp = await client.get(
            f"http://localhost:{telemetry_settings.jaeger_query_port}/api/traces/f13dc5318bf3bef64a0a5ea607db93a1",
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200 or len(resp.json()["data"][0]["spans"]) < 9:
            await asyncio.sleep(2)
        else:
            break

    assert resp.json()["data"][0]["traceID"] == "f13dc5318bf3bef64a0a5ea607db93a1"
    import pdb

    pdb.set_trace()
    assert len(resp.json()["data"][0]["spans"]) == 9
    assert len(resp.json()["data"][0]["processes"]) == 3


@pytest.mark.asyncio
async def test_telemetry_flat(http_service: AsyncClient, greeter: Greeter):
    resp = await http_service.get(
        "http://test/",
        headers={
            "b3": "80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1-05e3ac9a4f6e3b90c",
        },
    )
    assert resp.status_code == 200
    for i in range(10):
        if len(greeter.messages) == 0:
            await asyncio.sleep(1)
    assert (
        greeter.messages[0].headers["x-b3-traceid"]
        == "80f198ee56343ba864fe8b2a57d3eff7"
    )

    await asyncio.sleep(2)
    client = AsyncClient()
    for _ in range(10):
        resp = await client.get(
            f"http://localhost:{telemetry_settings.jaeger_query_port}/api/traces/80f198ee56343ba864fe8b2a57d3eff7",
            headers={"Accept": "application/json"},
        )
        if resp.status_code != 200 or len(resp.json()["data"][0]["spans"]) < 9:
            await asyncio.sleep(2)
        else:
            break

    assert resp.json()["data"][0]["traceID"] == "80f198ee56343ba864fe8b2a57d3eff7"
    assert len(resp.json()["data"][0]["spans"]) == 9
    assert len(resp.json()["data"][0]["processes"]) == 3
