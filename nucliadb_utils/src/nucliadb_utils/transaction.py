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
import asyncio
import uuid
from asyncio import Event
from functools import partial
from typing import Any, Optional, Union

import nats
import nats.errors
from nats.aio.client import Client
from nats.js.client import JetStreamContext

from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    BrokerMessageBlobReference,
    Notification,
    OpStatusWriter,
)
from nucliadb_telemetry.jetstream import JetStreamContextTelemetry
from nucliadb_utils import const, logger
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.nats import get_traced_jetstream
from nucliadb_utils.utilities import get_pubsub, has_feature


class WaitFor:
    uuid: str
    seq: Optional[int] = None

    def __init__(self, uuid: str, seq: Optional[int] = None):
        self.uuid = uuid
        self.seq = seq


class TransactionCommitTimeoutError(Exception):
    pass


class MaxTransactionSizeExceededError(Exception):
    pass


class LocalTransactionUtility:
    async def commit(
        self,
        writer: BrokerMessage,
        partition: int,
        wait: bool = False,
        target_subject: Optional[str] = None,
    ) -> int:
        from nucliadb_utils.utilities import get_ingest

        ingest = get_ingest()

        async def iterator(writer):
            yield writer

        resp = await ingest.ProcessMessage(iterator(writer))  # type: ignore
        if resp.status != OpStatusWriter.Status.OK:
            logger.error(f"Local transaction failed processing {writer}")
        return 0

    async def finalize(self):
        pass

    async def initialize(self):
        pass


class TransactionUtility:
    nc: Client
    js: Union[JetStreamContext, JetStreamContextTelemetry]
    pubsub: PubSubDriver

    def __init__(
        self,
        nats_servers: list[str],
        nats_creds: Optional[str] = None,
        commit_timeout: int = 60,
    ):
        self.nats_creds = nats_creds
        self.nats_servers = nats_servers
        self.commit_timeout = commit_timeout

    async def disconnected_cb(self):
        logger.info("Got disconnected from NATS!")

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.info("Got reconnected to NATS {url}".format(url=self.nc.connected_url))

    async def error_cb(self, e):
        logger.error(
            "There was an error connecting to NATS transaction: {}".format(e),
            exc_info=True,
        )

    async def closed_cb(self):
        logger.info("Connection is closed on NATS")

    async def stop_waiting(self, kbid: str, request_id: str):
        await self.pubsub.unsubscribe(
            key=const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=kbid),
            subscription_id=request_id,
        )

    def _get_notification_action_type(self):
        if has_feature(const.Features.WAIT_FOR_INDEX):
            return Notification.Action.INDEXED
        return Notification.Action.COMMIT  # currently do not handle ABORT!

    async def wait_for_commited(
        self, kbid: str, waiting_for: WaitFor, request_id: str
    ) -> Optional[Event]:
        action_type = self._get_notification_action_type()

        def received(waiting_for: WaitFor, event: Event, raw_data: bytes):
            data = self.pubsub.parse(raw_data)
            pb = Notification()
            pb.ParseFromString(data)
            if pb.uuid == waiting_for.uuid and pb.action == action_type:
                if (
                    waiting_for.seq is None
                    or pb.seqid == waiting_for.seq
                    # if we're waiting for index of this resource
                    # the seq id will not match
                    or action_type != Notification.Action.INDEXED
                ):
                    event.set()

        waiting_event = Event()
        partial_received = partial(received, waiting_for, waiting_event)
        await self.pubsub.subscribe(
            handler=partial_received,
            key=const.PubSubChannels.RESOURCE_NOTIFY.format(kbid=kbid),
            subscription_id=request_id,
        )
        return waiting_event

    async def initialize(self, service_name: Optional[str] = None):
        self.pubsub = await get_pubsub()  # type: ignore

        options: dict[str, Any] = {
            "error_cb": self.error_cb,
            "closed_cb": self.closed_cb,
            "reconnected_cb": self.reconnected_cb,
        }

        if self.nats_creds:
            options["user_credentials"] = self.nats_creds

        if len(self.nats_servers) > 0:
            options["servers"] = self.nats_servers

        self.nc = await nats.connect(**options)
        self.js = get_traced_jetstream(self.nc, service_name or "nucliadb")

    async def finalize(self):
        try:
            await self.nc.drain()
        except nats.errors.ConnectionClosedError:
            pass
        await self.nc.close()

    async def commit(
        self,
        writer: Union[BrokerMessage, BrokerMessageBlobReference],
        partition: int,
        wait: bool = False,
        target_subject: Optional[str] = None,  # allow customizing where to send the message
        headers: Optional[dict[str, str]] = None,
    ) -> int:
        headers = headers or {}
        waiting_event: Optional[Event] = None

        waiting_for = WaitFor(uuid=writer.uuid)
        request_id = uuid.uuid4().hex

        if wait:
            waiting_event = await self.wait_for_commited(writer.kbid, waiting_for, request_id=request_id)

        if target_subject is None:
            target_subject = const.Streams.INGEST.subject.format(partition=partition)

        try:
            res = await self.js.publish(target_subject, writer.SerializeToString(), headers=headers)
        except nats.errors.MaxPayloadError as ex:
            raise MaxTransactionSizeExceededError() from ex

        waiting_for.seq = res.seq

        if wait and waiting_event is not None:
            try:
                await asyncio.wait_for(waiting_event.wait(), timeout=self.commit_timeout)
            except asyncio.TimeoutError:
                logger.warning("Took too much to commit")
                raise TransactionCommitTimeoutError()
            finally:
                await self.stop_waiting(writer.kbid, request_id=request_id)

        logger.info(
            f" - Pushed message to ingest.  kb: {writer.kbid}, resource: {writer.uuid}, nucliadb seqid: {res.seq}, partition: {partition}"  # noqa
        )
        return res.seq
