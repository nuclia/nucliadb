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
import pytest
from httpx import AsyncClient

from nucliadb.common.cluster.discovery.chitchat import ChitchatAutoDiscovery
from nucliadb.common.cluster.manager import INDEX_NODES
from nucliadb.common.cluster.settings import Settings
from nucliadb_utils.tests import free_port


@pytest.fixture(scope="function")
async def chitchat_monitor_client():
    disc = ChitchatAutoDiscovery(
        Settings(chitchat_binding_host="0.0.0.0", chitchat_binding_port=free_port())
    )
    await disc.initialize()

    def make_client_fixture():
        client_base_url = "http://test"
        client = AsyncClient(app=disc.app, base_url=client_base_url)
        return client

    yield make_client_fixture

    await disc.finalize()


@pytest.mark.asyncio
async def test_chitchat_monitor(chitchat_monitor_client):
    INDEX_NODES.clear()
    async with chitchat_monitor_client() as client:
        member = dict(
            node_id=f"node",
            listen_addr=f"10.0.0.0",
            shard_count=20,
            type="Io",
            is_self=False,
        )
        response = await client.patch("/members", json=[member])
        assert response.status_code == 204
    assert len(INDEX_NODES) == 1
