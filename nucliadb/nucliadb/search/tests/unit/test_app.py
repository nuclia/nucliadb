from unittest.mock import patch

import pytest

from nucliadb.search import app

pytestmark = pytest.mark.asyncio


async def test_alive():
    with patch.object(app, "NODES", {"node1": "node1"}):
        resp = await app.alive(None)
        assert resp.status_code == 200


async def test_not_alive():
    with patch.object(app, "NODES", {}):
        resp = await app.alive(None)
        assert resp.status_code == 503


async def test_ready():
    with patch.object(app, "NODES", {"node1": "node1"}):
        resp = await app.ready(None)
        assert resp.status_code == 200


async def test_not_ready():
    with patch.object(app, "NODES", {}):
        resp = await app.ready(None)
        assert resp.status_code == 503
