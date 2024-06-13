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
from unittest.mock import AsyncMock, Mock


def get_mocked_session(
    http_method: str, status: int, text=None, json=None, read=None, context_manager=True
):
    response = Mock(status=status)
    if text is not None:
        response.text = AsyncMock(return_value=text)
    if json is not None:
        response.json = AsyncMock(return_value=json)
    if read is not None:
        if isinstance(read, str):
            read = read.encode()
        response.read = AsyncMock(return_value=read)
    if context_manager:
        # For when async with self.session.post() as response: is called
        session = Mock()
        http_method_mock = AsyncMock(__aenter__=AsyncMock(return_value=response))
        getattr(session, http_method.lower()).return_value = http_method_mock
    else:
        # For when await self.session.post() is called
        session = AsyncMock()
        getattr(session, http_method.lower()).return_value = response
    return session
