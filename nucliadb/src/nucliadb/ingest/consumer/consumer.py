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
import logging
import time
from typing import Optional, Union

import backoff
import nats
import nats.js.api
from nats.aio.client import Msg

from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.exceptions import ConflictError
from nucliadb.ingest import logger
from nucliadb.ingest.orm.exceptions import DeadletteredError, SequenceOrderViolation
from nucliadb.ingest.orm.processor import Processor, sequence_manager
from nucliadb_protos.writer_pb2 import BrokerMessage, BrokerMessageBlobReference
from nucliadb_telemetry import context, errors, metrics
from nucliadb_utils import const
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.nats import MessageProgressUpdater, NatsConnectionManager
from nucliadb_utils.settings import nats_consumer_settings
from nucliadb_utils.storages.storage import Storage

consumer_observer = metrics.Observer(
    "message_processor",
    labels={"source": ""},
    buckets=[
        0.01,
        0.025,
        0.05,
        0.1,
        0.5,
        1.0,
        2.5,
        5.0,
        7.5,
        10.0,
        30.0,
        60.0,
        120.0,
        300.0,
        float("inf"),
    ],
    error_mappings={"deadlettered": DeadletteredError, "shardnotfound": ShardsNotFound},
)


class IngestConsumer:
    def __init__(
        self,
        driver: Driver,
        partition: str,
        storage: Storage,
        nats_connection_manager: NatsConnectionManager,
        pubsub: Optional[PubSubDriver] = None,
        lock: Optional[Union[asyncio.Lock, asyncio.Semaphore]] = None,
    ):
        self.driver = driver
        self.partition = partition
        self.nats_connection_manager = nats_connection_manager
        self.storage = storage
        self.initialized = False

        self.lock = lock or asyncio.Lock()
        self.processor = Processor(driver, storage, pubsub, partition)

    async def initialize(self):
        await self.setup_nats_subscription()
        self.initialized = True

    async def setup_nats_subscription(self):
        last_seqid = await sequence_manager.get_last_seqid(self.driver, self.partition)
        if last_seqid is None:
            last_seqid = 1
        subject = const.Streams.INGEST.subject.format(partition=self.partition)
        await self.nats_connection_manager.subscribe(
            subject=subject,
            queue=const.Streams.INGEST.group.format(partition=self.partition),
            stream=const.Streams.INGEST.name,
            flow_control=True,
            cb=self.subscription_worker,
            subscription_lost_cb=self.setup_nats_subscription,
            config=nats.js.api.ConsumerConfig(
                deliver_policy=nats.js.api.DeliverPolicy.BY_START_SEQUENCE,
                opt_start_seq=last_seqid,
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_ack_pending=nats_consumer_settings.nats_max_ack_pending,
                max_deliver=nats_consumer_settings.nats_max_deliver,
                ack_wait=nats_consumer_settings.nats_ack_wait,
                idle_heartbeat=nats_consumer_settings.nats_idle_heartbeat,
            ),
        )
        logger.info(f"Subscribed to {subject} on stream {const.Streams.INGEST.name} from {last_seqid}")

    @backoff.on_exception(backoff.expo, (ConflictError,), jitter=backoff.random_jitter, max_tries=4)
    async def _process(self, pb: BrokerMessage, seqid: int):
        await self.processor.process(pb, seqid, self.partition)

    async def get_broker_message(self, msg: Msg) -> BrokerMessage:
        pb_data = msg.data
        if msg.headers is not None and msg.headers.get("X-MESSAGE-TYPE") == "PROXY":
            # this is a message that is referencing a blob because
            # it was too big to be sent through NATS
            ref_pb = BrokerMessageBlobReference()
            ref_pb.ParseFromString(pb_data)
            pb_data = await self.storage.get_stream_message(ref_pb.storage_key)
        pb = BrokerMessage()
        pb.ParseFromString(pb_data)
        return pb

    async def clean_broker_message(self, msg: Msg) -> None:
        if msg.headers is not None and msg.headers.get("X-MESSAGE-TYPE") == "PROXY":
            ref_pb = BrokerMessageBlobReference()
            ref_pb.ParseFromString(msg.data)
            try:
                await self.storage.del_stream_message(ref_pb.storage_key)
            except Exception:  # pragma: no cover
                logger.warning("Could not delete blob reference", exc_info=True)

    async def subscription_worker(self, msg: Msg):
        subject = msg.subject
        reply = msg.reply
        seqid = int(reply.split(".")[5])
        message_source = "<msg source not set>"
        start = time.monotonic()

        async with (
            MessageProgressUpdater(msg, nats_consumer_settings.nats_ack_wait * 0.66),
            self.lock,
        ):
            logger.info(f"Message processing: subject:{subject}, seqid: {seqid}, reply: {reply}")
            try:
                pb = await self.get_broker_message(msg)
                if pb.source == pb.MessageSource.PROCESSOR:
                    message_source = "processing"
                elif pb.source == pb.MessageSource.WRITER:
                    message_source = "writer"
                if pb.HasField("audit"):
                    audit_time = pb.audit.when.ToDatetime().isoformat()
                else:
                    audit_time = ""

                logger.debug(
                    f"Received from {message_source} on {pb.kbid}/{pb.uuid} seq {seqid} partition {self.partition} at {time}"  # noqa
                )
                context.add_context({"kbid": pb.kbid, "rid": pb.uuid})

                try:
                    with consumer_observer(
                        {"source": ("writer" if pb.source == pb.MessageSource.WRITER else "processor")}
                    ):
                        await self._process(pb, seqid)
                except SequenceOrderViolation as err:
                    logger.log(
                        level=logging.ERROR if seqid < err.last_seqid else logging.WARNING,
                        msg="Old txn. Discarding message",
                        extra={
                            "stored_seqid": err.last_seqid,
                            "message_seqid": seqid,
                            "partition": self.partition,
                            "kbid": pb.kbid,
                            "msg_delivered_count": msg.metadata.num_delivered,
                        },
                    )
                else:
                    message_type_name = pb.MessageType.Name(pb.type)
                    time_to_process = time.monotonic() - start
                    log_level = logging.INFO if time_to_process < 10 else logging.WARNING
                    logger.log(
                        log_level,
                        f"Successfully processed {message_type_name} message",
                        extra={
                            "kbid": pb.kbid,
                            "rid": pb.uuid,
                            "message_source": message_source,
                            "nucliadb_seqid": seqid,
                            "partition": self.partition,
                            "total_time": time_to_process,
                            "audit_time": audit_time,
                        },
                    )
            except DeadletteredError as e:
                # Messages that have been sent to deadletter at some point
                # We don't want to process it again so it's ack'd
                errors.capture_exception(e)
                logger.info(
                    f"An error happend while processing a message from {message_source}. "
                    f"A copy of the message has been stored on {self.processor.storage.deadletter_bucket}. "
                    f"Check sentry for more details: {str(e)}"
                )
                await msg.ack()
            except (ShardsNotFound,) as e:
                # Any messages that for some unexpected inconsistency have failed and won't be tried again
                # as we cannot do anything about it
                # - ShardsNotFound: /kb/{id}/shards key or the whole /kb/{kbid} is missing
                errors.capture_exception(e)
                logger.info(
                    f"An error happend while processing a message from {message_source}. "
                    f"This message has been dropped and won't be retried again"
                    f"Check sentry for more details: {str(e)}"
                )
                await msg.ack()
            except Exception as e:
                # Unhandled exceptions that need to be retried after a small delay
                errors.capture_exception(e)
                logger.exception(
                    f"An error happend while processing a message from {message_source}. "
                    "Message has not been ACKd and will be retried. "
                    f"Check sentry for more details: {str(e)}"
                )
                await msg.nak()
                raise e
            else:
                # Successful processing
                await msg.ack()
                await self.clean_broker_message(msg)


class IngestProcessedConsumer(IngestConsumer):
    """
    Consumer designed to write processed resources to the database.

    This is so that we can have a single consumer for both the regular writer and writes
    coming from processor.

    This is important because writes coming from processor can be very large and slow and
    other writes are going to be coming from user actions and we don't want to slow them down.
    """

    async def setup_nats_subscription(self):
        subject = const.Streams.INGEST_PROCESSED.subject
        await self.nats_connection_manager.subscribe(
            subject=subject,
            queue=const.Streams.INGEST_PROCESSED.group,
            stream=const.Streams.INGEST_PROCESSED.name,
            flow_control=True,
            cb=self.subscription_worker,
            subscription_lost_cb=self.setup_nats_subscription,
            config=nats.js.api.ConsumerConfig(
                ack_policy=nats.js.api.AckPolicy.EXPLICIT,
                max_ack_pending=100,  # custom ack pending here
                max_deliver=nats_consumer_settings.nats_max_deliver,
                ack_wait=nats_consumer_settings.nats_ack_wait,
                idle_heartbeat=nats_consumer_settings.nats_idle_heartbeat,
            ),
        )
        logger.info(f"Subscribed to {subject} on stream {const.Streams.INGEST_PROCESSED.name}")

    @backoff.on_exception(backoff.expo, (ConflictError,), jitter=backoff.random_jitter, max_tries=4)
    async def _process(self, pb: BrokerMessage, seqid: int):
        """
        We are setting `transaction_check` to False here because we can not mix
        transaction ids from regular ingest writes and writes coming from processor.
        """
        await self.processor.process(pb, seqid, self.partition, transaction_check=False)
