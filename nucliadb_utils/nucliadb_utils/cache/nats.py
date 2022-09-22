# -*- coding: utf-8 -*-
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

import asyncio
import os
import uuid
from typing import Dict, List, Optional

import nats
import nats.errors
from nats.aio.client import Client
from nats.aio.errors import ErrConnectionClosed, ErrNoServers, ErrTimeout
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js.client import JetStreamContext  # type: ignore
from nats.js.manager import JetStreamManager

from nucliadb_utils import logger
from nucliadb_utils.cache.pubsub import PubSubDriver


async def wait_for_it(future: asyncio.Future, msg):
    future.set_result(msg)


# Configuration Utility


class NatsPubsub(PubSubDriver):
    _jetstream = None
    _jsm = None
    _subscriptions: Dict[str, Subscription]
    async_callback = True

    def __init__(
        self,
        thread: bool = False,
        name: Optional[str] = "natsutility",
        timeout: float = 2.0,
        hosts: Optional[List[str]] = None,
        user_credentials_file: Optional[str] = None,
    ):
        self._hosts = hosts or []
        self._timeout = timeout
        self._subscriptions = {}
        self._name = name
        self._thread = thread
        self._uuid = os.environ.get("HOSTNAME", uuid.uuid4().hex)
        self.initialized = False
        self.lock = asyncio.Lock()
        self.nc = None
        self.user_credentials_file = user_credentials_file

    @property
    def jetstream(self) -> JetStreamContext:
        if self.nc is None:
            raise AttributeError("NC not initialized")
        if self._jetstream is None:
            self._jetstream = self.nc.jetstream()
        return self._jetstream

    @property
    def jsm(self) -> JetStreamManager:
        if self.nc is None:
            raise AttributeError("NC not initialized")
        if self._jsm is None:
            self._jsm = self.nc.jsm()
        return self._jsm

    async def initialize(self):
        # No asyncio loop to run

        async with self.lock:
            self.nc = Client()
            options = {
                "servers": self._hosts,
                "disconnected_cb": self.disconnected_cb,
                "reconnected_cb": self.reconnected_cb,
                "error_cb": self.error_cb,
                "closed_cb": self.closed_cb,
                "name": self._name,
                "verbose": True,
            }
            if self.user_credentials_file is not None:
                options["user_credentials"] = self.user_credentials_file
            try:
                await self.nc.connect(**options)
            except ErrNoServers:
                logger.exception("No servers found")
                raise

            logger.info("Connected to nats")

        self.initialized = True

    async def finalize(self):
        if self.nc:
            for subscription in self._subscriptions.values():
                await subscription.unsubscribe()

            try:
                await self.nc.drain()
            except RuntimeError:
                pass
            except nats.errors.ConnectionClosedError:
                pass

            try:
                await self.nc.close()
            except RuntimeError:
                pass
            except AttributeError:
                pass
        self.initialized = False

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.info(
            "Got reconnected NATS to {url}".format(url=self.nc.connected_url.netloc)
        )

    async def error_cb(self, e):
        logger.info("There was an error connecting to NATS {}".format(e))

    async def closed_cb(self):
        logger.info("Connection is closed to NATS")

    async def request(self, key, value, timeout=None):
        if timeout is None:
            timeout = self._timeout
        if self.nc is not None and self.nc.is_connected:
            try:
                return await self.nc.request(key, value, timeout)
            except ErrTimeout:
                return
        else:
            raise ErrConnectionClosed("Could not subscribe")

    async def subscribe(self, handler, key, group=""):
        if self.nc is not None and self.nc.is_connected:
            sid = await self.nc.subscribe(key, queue=group, cb=handler)
            self._subscriptions[key] = sid
            logger.info("Subscribed to " + key)
            return sid
        else:
            raise ErrConnectionClosed("Could not subscribe")

    async def unsubscribe(self, key: str):
        if key in self._subscriptions:
            await self._subscriptions[key].unsubscribe()
            del self._subscriptions[key]
        else:
            raise KeyError(f"No subscription at {key}")

    async def publish(self, key, value):
        if self.nc is not None and self.nc.is_connected:
            await self.nc.publish(key, value)
        else:
            raise ErrConnectionClosed("Could not publish")

    def parse(self, data: Msg):
        return data.data
