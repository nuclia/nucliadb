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
import logging
import sys
import time
from functools import cached_property, partial
from typing import Any, Awaitable, Callable, Optional, Union

import nats
import nats.errors
import nats.js.api
from nats.aio.client import Client as NATSClient
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js.client import JetStreamContext

from nucliadb_telemetry.jetstream import (
    JetStreamContextTelemetry,
    NatsClientTelemetry,
    get_traced_nats_client,
)
from nucliadb_telemetry.metrics import Counter
from nucliadb_utils.helpers import MessageProgressUpdater

logger = logging.getLogger(__name__)

# Re-export for bw/c. This function was defined here but makes more sense in the
# telemetry library
from nucliadb_telemetry.jetstream import get_traced_jetstream  # noqa


class NatsMessageProgressUpdater(MessageProgressUpdater):
    """
    Context manager to send progress updates to NATS.

    This should allow lower ack_wait time settings without causing
    messages to be redelivered.
    """

    def __init__(self, msg: Msg, timeout: float):
        async def update_msg() -> bool:
            if msg._ackd:  # all done, do not mark with in_progress
                return True
            await msg.in_progress()
            return False

        seqid = msg.reply.split(".")[5]
        super().__init__(seqid, update_msg, timeout)


class NatsConnectionManager:
    _nc: Union[NATSClient, NatsClientTelemetry]
    _subscriptions: list[tuple[Subscription, Callable[[], Awaitable[None]]]]
    _pull_subscriptions: list[
        tuple[
            JetStreamContext.PullSubscription, asyncio.Task, Callable[[], Awaitable[None]], asyncio.Event
        ]
    ]
    _unhealthy_timeout = 10  # needs to be unhealth for 10 seconds to be unhealthy and force exit

    def __init__(
        self,
        *,
        service_name: str,
        nats_servers: list[str],
        nats_creds: Optional[str] = None,
        pull_utilization_metrics: Optional[Counter] = None,
    ):
        self._service_name = service_name
        self._nats_servers = nats_servers
        self._nats_creds = nats_creds
        self._subscriptions = []
        self._pull_subscriptions = []
        self._lock = asyncio.Lock()
        self._healthy = True
        self._last_unhealthy: Optional[float] = None
        self._needs_reconnection = False
        self._reconnect_task: Optional[asyncio.Task] = None
        self._expected_subscriptions: set[str] = set()
        self._initialized = False
        self.pull_utilization_metrics = pull_utilization_metrics

    def healthy(self) -> bool:
        if not self._healthy:
            return False

        if (
            self._last_unhealthy is not None
            and time.monotonic() - self._last_unhealthy > self._unhealthy_timeout
        ):
            return False

        if not self._nc.is_connected:
            self._last_unhealthy = time.monotonic()

        return True

    async def initialize(self) -> None:
        if self._initialized:
            return

        options: dict[str, Any] = {
            "error_cb": self.error_cb,
            "closed_cb": self.closed_cb,
            "reconnected_cb": self.reconnected_cb,
            "disconnected_cb": self.disconnected_cb,
        }

        if self._nats_creds:
            options["user_credentials"] = self._nats_creds

        if len(self._nats_servers) > 0:
            options["servers"] = self._nats_servers

        async with self._lock:
            nc = await nats.connect(**options)
            self._nc = get_traced_nats_client(nc, self._service_name)

        self._expected_subscription_task = asyncio.create_task(self._verify_expected_subscriptions())

        self._initialized = True

    async def finalize(self):
        if not self._initialized:
            return

        async with self._lock:
            if self._reconnect_task:
                self._reconnect_task.cancel()
                try:
                    await self._reconnect_task
                except asyncio.CancelledError:
                    pass

            self._expected_subscription_task.cancel()
            try:
                await self._expected_subscription_task
            except asyncio.CancelledError:
                pass

            # Finalize push subscriptions
            for sub, _ in self._subscriptions:
                try:
                    await sub.drain()
                except nats.errors.ConnectionClosedError:  # pragma: no cover
                    pass
            self._subscriptions = []

            # Finalize pull subscriptions
            for pull_sub, task, _, cancelled in self._pull_subscriptions:
                cancelled.set()
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                await pull_sub.unsubscribe()
            self._pull_subscriptions = []

            # close the connection
            try:
                await asyncio.wait_for(self._nc.drain(), timeout=1)
            except (
                nats.errors.ConnectionReconnectingError,
                nats.errors.ConnectionClosedError,
                asyncio.TimeoutError,
            ):  # pragma: no cover
                pass
            await self._nc.close()

        self._initialized = False

    async def disconnected_cb(self) -> None:
        logger.info("Disconnected from NATS!")
        self._last_unhealthy = time.monotonic()

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.warning(
            f"Reconnected to NATS {self._nc.connected_url.netloc}. Attempting to re-subscribe."
        )
        # This callback may be interrupted by NATS. In order to avoid being interrupted, we spawn our own task to do
        # the reconnection. If we are interrupted we could lose subscriptions
        self._needs_reconnection = True
        if self._reconnect_task is None:
            self._reconnect_task = asyncio.create_task(self._reconnect())

    async def _reconnect(self):
        tries = 0
        # Loop in case we receive another reconnection request while one is ongoing
        while self._needs_reconnection:
            if tries > 5:
                logger.error(
                    "Too many consecutive reconnection to NATS. Something might be wrong. Exiting."
                )
                sys.exit(1)

            try:
                self._needs_reconnection = False

                async with self._lock:
                    existing_subs = self._subscriptions
                    self._subscriptions = []
                    for sub, recon_callback in existing_subs:
                        try:
                            await sub.drain()
                            await recon_callback()
                        except Exception:
                            logger.exception(
                                f"Error resubscribing to {sub.subject} on {self._nc.connected_url.netloc}"
                            )
                            # should force exit here to restart the service
                            self._healthy = False
                            raise

                    existing_pull_subs = self._pull_subscriptions
                    self._pull_subscriptions = []
                    for pull_sub, task, recon_callback, cancelled in existing_pull_subs:
                        cancelled.set()
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                        try:
                            await pull_sub.unsubscribe()
                            await recon_callback()
                        except Exception:
                            logger.exception(
                                f"Error resubscribing to pull subscription on {self._nc.connected_url.netloc}"
                            )
                            # should force exit here to restart the service
                            self._healthy = False
                            raise
                self._healthy = True
                self._last_unhealthy = None  # reset the last unhealthy time
            except Exception:
                if self._needs_reconnection:
                    logger.warning("Error reconnecting to NATS, will retry")
                else:
                    logger.exception("Error reconnecting to NATS. Exiting.")
                    sys.exit(1)

            if self._needs_reconnection:
                logger.warning(
                    "While reconnecting to NATS subscriptions, a reconnect request was received. Reconnecting again.",
                )
            tries += 1

        self._reconnect_task = None

    async def error_cb(self, e):  # pragma: no cover
        logger.error(f"There was an error on consumer: {e}", exc_info=e)

    async def closed_cb(self):  # pragma: no cover
        logger.info("Connection is closed on NATS")

    @property
    def nc(self) -> Union[NATSClient, NatsClientTelemetry]:
        return self._nc

    @cached_property
    def js(self) -> Union[JetStreamContext, JetStreamContextTelemetry]:
        return get_traced_jetstream(self._nc, self._service_name)

    async def subscribe(
        self,
        *,
        subject: str,
        queue: str,
        stream: str,
        cb: Callable[[Msg], Awaitable[None]],
        subscription_lost_cb: Callable[[], Awaitable[None]],
        flow_control: bool = False,
        manual_ack: bool = True,
        config: Optional[nats.js.api.ConsumerConfig] = None,
    ) -> Subscription:
        sub = await self.js.subscribe(
            subject=subject,
            queue=queue,
            stream=stream,
            cb=cb,
            flow_control=flow_control,
            manual_ack=manual_ack,
            config=config,
        )

        self._subscriptions.append((sub, subscription_lost_cb))

        return sub

    async def pull_subscribe(
        self,
        *,
        subject: str,
        stream: str,
        cb: Callable[[Msg], Awaitable[None]],
        subscription_lost_cb: Callable[[], Awaitable[None]],
        durable: Optional[str] = None,
        config: Optional[nats.js.api.ConsumerConfig] = None,
    ) -> JetStreamContext.PullSubscription:
        wrapped_cb: Callable[[Msg], Awaitable[None]]
        if isinstance(self.js, JetStreamContextTelemetry):
            wrapped_cb = partial(self.js.trace_pull_subscriber_message, cb)
        else:
            wrapped_cb = cb

        psub = await self.js.pull_subscribe(
            subject,
            durable=durable,
            stream=stream,
            config=config,
        )
        cancelled = asyncio.Event()

        async def consume(psub: JetStreamContext.PullSubscription, subject: str):
            while True:
                if cancelled.is_set():
                    break
                try:
                    if self.pull_utilization_metrics:
                        start_wait = time.monotonic()

                    messages = await psub.fetch(batch=1)

                    if self.pull_utilization_metrics:
                        received = time.monotonic()
                        self.pull_utilization_metrics.inc({"status": "waiting"}, received - start_wait)

                    for message in messages:
                        await wrapped_cb(message)

                    if self.pull_utilization_metrics:
                        processed = time.monotonic()
                        self.pull_utilization_metrics.inc({"status": "processing"}, processed - received)
                except asyncio.CancelledError:
                    # Handle task cancellation
                    logger.info("Pull subscription consume task cancelled", extra={"subject": subject})
                    break
                except TimeoutError:
                    if self.pull_utilization_metrics:
                        received = time.monotonic()
                        self.pull_utilization_metrics.inc({"status": "waiting"}, received - start_wait)
                except Exception:
                    logger.exception("Error in pull_subscribe task", extra={"subject": subject})

        task = asyncio.create_task(consume(psub, subject), name=f"pull_subscribe_{subject}")
        self._pull_subscriptions.append((psub, task, subscription_lost_cb, cancelled))

        if durable:
            self._expected_subscriptions.add(durable)

        return psub

    async def _remove_subscription(
        self, subscription: Union[Subscription, JetStreamContext.PullSubscription]
    ):
        async with self._lock:
            for index, (sub, _) in enumerate(self._subscriptions):
                if sub is not subscription:
                    continue
                self._subscriptions.pop(index)
                return
            for index, (psub, task, _, cancelled) in enumerate(self._pull_subscriptions):
                if psub is not subscription:
                    continue
                self._pull_subscriptions.pop(index)
                cancelled.set()
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                return

    async def unsubscribe(self, subscription: Union[Subscription, JetStreamContext.PullSubscription]):
        await subscription.unsubscribe()
        await self._remove_subscription(subscription)

        if isinstance(subscription, JetStreamContext.PullSubscription):
            self._expected_subscriptions.remove(subscription._consumer)  # type: ignore

    async def _verify_expected_subscriptions(self):
        failures = 0
        while True:
            await asyncio.sleep(30)

            existing_subs = set(sub._consumer for sub, _, _, _ in self._pull_subscriptions)
            missing_subs = self._expected_subscriptions - existing_subs
            if missing_subs:
                logger.warning(f"Some NATS subscriptions are missing {missing_subs}")
                failures += 1
            else:
                failures = 0

            if failures >= 3:
                logger.warning("Some NATS subscriptions are missing for too long, exiting")
                sys.exit(1)
