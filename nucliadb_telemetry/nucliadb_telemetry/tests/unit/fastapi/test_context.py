from nucliadb_telemetry.fastapi.context import ContextInjectorMiddleware
from unittest.mock import AsyncMock
from nucliadb_telemetry import context
from fastapi import FastAPI
import pytest
from starlette.requests import Request

app = FastAPI()


@app.get("/api/v1/kb/{kbid}")
def get_kb(kbid: str):
    return {"kbid": kbid}


@pytest.mark.asyncio
async def test_context_injected():
    scope = {
        "app": app,
        "path": "/api/v1/kb/123",
        "method": "GET",
        "type": "http",
    }

    mdlw = ContextInjectorMiddleware(app)

    found_ctx = {}

    async def receive(*args, **kwargs):
        found_ctx.update(context.get_context())
        return {
            "type": "http.disconnect",
        }

    await mdlw(scope, receive, AsyncMock())

    assert found_ctx == {"kbid": "123"}
