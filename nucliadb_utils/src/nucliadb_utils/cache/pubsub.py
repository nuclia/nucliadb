# Copyright 2021 Bosutech XXI S.L.
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
from collections.abc import Awaitable, Callable
from typing import Generic, TypeVar

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

    async def unsubscribe(self, key: str, subscription_id: str | None = None):
        raise NotImplementedError()

    async def subscribe(
        self,
        handler: Callback,
        key: str,
        group: str | None = None,
        subscription_id: str | None = None,
    ):
        raise NotImplementedError()

    def parse(self, data: T) -> bytes:
        raise NotImplementedError()
