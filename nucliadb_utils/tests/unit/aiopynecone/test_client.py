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
import unittest

from nucliadb_utils.aiopynecone.client import (
    PineconeSession,
)


async def test_session():
    session = PineconeSession()
    client = session.get_client(api_key="api_key")
    assert client.session is session.http_session

    client2 = session.get_client(api_key="api_key_2")
    assert client2.session is session.http_session

    await session.finalize()


async def test_create_index():
    session = PineconeSession()
    response_mock = unittest.mock.Mock()
    response_mock.raise_for_status.return_value = None
    response_mock.json.return_value = {"host": "host"}
    session_mock = unittest.mock.Mock()
    session_mock.post = unittest.mock.AsyncMock(return_value=response_mock)
    session.http_session = session_mock

    client = session.get_client(api_key="api_key")

    host = await client.create_index(name="name", dimension=1)
    assert host == "host"
    session_mock.post.assert_called_once()


async def test_delete_index():
    session = PineconeSession()
    response_mock = unittest.mock.Mock()
    response_mock.raise_for_status.return_value = None
    session_mock = unittest.mock.Mock()
    session_mock.delete = unittest.mock.AsyncMock(return_value=response_mock)
    session.http_session = session_mock

    client = session.get_client(api_key="api_key")

    await client.delete_index(name="name")
    session_mock.delete.assert_called_once()
