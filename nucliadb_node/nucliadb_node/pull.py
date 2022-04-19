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

# We need to pull from jetstream key partition

import asyncio
from typing import List

import nats
from nats.aio.client import Msg
from nats.aio.subscription import Subscription
from nucliadb_protos.noderesources_pb2 import Resource, ResourceID
from nucliadb_protos.nodewriter_pb2 import IndexMessage
from sentry_sdk import capture_exception

from nucliadb_node import logger
from nucliadb_node.reader import Reader
from nucliadb_node.sentry import SENTRY
from nucliadb_node.writer import Writer
from nucliadb_utils.settings import indexing_settings
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_storage,
    get_transaction,
)


class Worker:
    subscriptions: List[Subscription]

    def __init__(
        self,
        writer: Writer,
        reader: Reader,
        node: str,
    ):
        self.writer = writer
        self.reader = reader
        self.subscriptions = []
        self.ack_wait = 5
        self.lock = asyncio.Lock()
        self.node = node

    async def finalize(self):
        for subscription in self.subscriptions:
            try:
                await subscription.drain()
            except nats.errors.ConnectionClosedError:
                pass
        self.subscriptions = []
        transaction_utility = get_transaction()
        if transaction_utility:
            await transaction_utility.finalize()
            clean_utility(Utility.TRANSACTION)

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.info("Got reconnected to NATS {url}".format(url=self.nc.connected_url))

    async def error_cb(self, e):
        logger.error("There was an error connecting to NATS: {}".format(e))

    async def closed_cb(self):
        logger.info("Connection is closed on NATS")

    async def initialize(self):

        options = {
            "error_cb": self.error_cb,
            "closed_cb": self.closed_cb,
            "reconnected_cb": self.reconnected_cb,
        }

        if indexing_settings.index_jetstream_auth is not None:
            options["user_credentials"] = indexing_settings.index_jetstream_auth

        if len(indexing_settings.index_jetstream_servers) > 0:
            options["servers"] = indexing_settings.index_jetstream_servers

        self.nc = await nats.connect(**options)

        logger.info(f"Nats: Connected to {indexing_settings.index_jetstream_servers}")
        self.js = self.nc.jetstream()
        await self.subscribe()

    async def subscription_worker(self, msg: Msg):
        subject = msg.subject
        reply = msg.reply
        seqid = int(msg.reply.split(".")[5])
        logger.info(
            f"Message received: subject:{subject}, seqid: {seqid}, reply: {reply}"
        )
        storage = await get_storage()
        async with self.lock:
            try:
                pb = IndexMessage()
                pb.ParseFromString(msg.data)
                if pb.typemessage == IndexMessage.TypeMessage.CREATION:
                    brain: Resource = await storage.get_indexing(pb)
                    status = await self.writer.set_resource(brain)
                    del brain
                elif pb.typemessage == IndexMessage.TypeMessage.DELETION:
                    rid = ResourceID()
                    rid.shard_id = pb.shard
                    rid.uuid = pb.resource
                    status = await self.writer.delete_resource(rid)
                self.reader.update(pb.shard, status)

                logger.info("Processed")

            except Exception as e:
                if SENTRY:
                    capture_exception(e)
                logger.error(
                    f"An error on subscription_worker. Check sentry for more details. {str(e)}"
                )
                raise e
        await msg.ack()
        await storage.delete_indexing(pb)

    async def subscribe(self):

        try:
            await self.js.stream_info(indexing_settings.index_jetstream_stream)
        except nats.js.errors.NotFoundError:
            logger.info("Creating stream")
            res = await self.js.add_stream(
                name=indexing_settings.index_jetstream_stream,
                subjects=[indexing_settings.index_jetstream_target.format(node=">")],
            )
            await self.js.stream_info(indexing_settings.index_jetstream_stream)

        res = await self.js.subscribe(
            subject=indexing_settings.index_jetstream_target.format(node=self.node),
            queue=indexing_settings.index_jetstream_group.format(node=self.node),
            stream=indexing_settings.index_jetstream_stream,
            flow_control=True,
            cb=self.subscription_worker,
            config=nats.js.api.ConsumerConfig(
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_deliver=1,
                ack_wait=self.ack_wait,
                idle_heartbeat=5,
            ),
        )
        self.subscriptions.append(res)
        logger.info(
            f"Subscribed to {indexing_settings.index_jetstream_target.format(node=self.node)} on \
             stream {indexing_settings.index_jetstream_stream}"
        )
