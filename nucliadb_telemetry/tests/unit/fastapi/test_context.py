# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from unittest.mock import patch

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from nucliadb_telemetry.fastapi.context import ContextInjectorMiddleware

app = FastAPI()


@app.get("/api/v1/kb/{kbid}")
def get_kb(kbid: str):
    return {"kbid": kbid}


async def test_context_injected():
    app.add_middleware(ContextInjectorMiddleware)

    transport = ASGITransport(app=app)  # type: ignore
    client = AsyncClient(transport=transport, base_url="http://test/api/v1")

    with patch("nucliadb_telemetry.fastapi.context.context.add_context") as add_context:
        await client.get("/kb/123", headers={"User-Agent": "test-agent/1.0"})
        assert add_context.call_count == 1
        context_data = add_context.call_args[0][0]
        assert context_data["kbid"] == "123"
        assert context_data["user_agent"] == "test-agent/1.0"


async def test_context_without_user_agent():
    """Test that context still works when user agent is not provided"""
    app.add_middleware(ContextInjectorMiddleware)

    transport = ASGITransport(app=app)  # type: ignore
    client = AsyncClient(transport=transport, base_url="http://test/api/v1")

    with patch("nucliadb_telemetry.fastapi.context.context.add_context") as add_context:
        await client.get("/kb/456")
        assert add_context.call_count == 1
        context_data = add_context.call_args[0][0]
        assert context_data == {"kbid": "456"}
