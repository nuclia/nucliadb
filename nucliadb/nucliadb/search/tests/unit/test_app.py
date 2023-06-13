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

from unittest.mock import patch

import pytest

from nucliadb.search import app

pytestmark = pytest.mark.asyncio


async def test_alive():
    with patch.object(app.manager, "get_index_nodes", return_value={"node1": "node1"}):
        resp = await app.alive(None)
        assert resp.status_code == 200


async def test_not_alive():
    with patch.object(app.manager, "get_index_nodes", return_value={}):
        resp = await app.alive(None)
        assert resp.status_code == 503


async def test_ready():
    with patch.object(app.manager, "get_index_nodes", return_value={"node1": "node1"}):
        resp = await app.ready(None)
        assert resp.status_code == 200


async def test_not_ready():
    with patch.object(app.manager, "get_index_nodes", return_value={}):
        resp = await app.ready(None)
        assert resp.status_code == 503
