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
from typing import Awaitable, Callable, Generic, Optional, TypeVar

T = TypeVar("T")

Callback = Callable[[T], Awaitable[None]]


class PubSubDriver(Generic[T]):
    initialized: bool = False
    async_callback: bool = False

    async def initialize(self):
        raise NotImplementedError()

    async def finalize(self):
        raise NotImplementedError()

    async def publish(self, channel_name: str, data: bytes):
        raise NotImplementedError()

    async def unsubscribe(self, key: str, subscription_id: Optional[str] = None):
        raise NotImplementedError()

    async def subscribe(
        self,
        handler: Callback,
        key: str,
        group: Optional[str] = None,
        subscription_id: Optional[str] = None,
    ):
        raise NotImplementedError()

    def parse(self, data: T) -> bytes:
        raise NotImplementedError()
