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
import time
from functools import cached_property
from typing import Any, Awaitable, Callable, Optional, Union

import nats
import nats.errors
import nats.js.api
from nats.aio.client import Client as NATSClient
from nats.aio.client import Msg
from nats.aio.subscription import Subscription
from nats.js.client import JetStreamContext

from nucliadb_telemetry.errors import capture_exception
from nucliadb_telemetry.jetstream import JetStreamContextTelemetry
from nucliadb_telemetry.utils import get_telemetry

logger = logging.getLogger(__name__)


def get_traced_jetstream(
    nc: NATSClient, service_name: str
) -> Union[JetStreamContext, JetStreamContextTelemetry]:
    jetstream = nc.jetstream()
    tracer_provider = get_telemetry(service_name)

    if tracer_provider is not None and jetstream is not None:  # pragma: no cover
        logger.info(f"Configuring {service_name} jetstream with telemetry")
        jetstream = JetStreamContextTelemetry(jetstream, service_name, tracer_provider)
    return jetstream


class MessageProgressUpdater:
    """
    Context manager to send progress updates to NATS.

    This should allow lower ack_wait time settings without causing
    messages to be redelivered.
    """

    _task: asyncio.Task

    def __init__(self, msg: Msg, timeout: float):
        self.msg = msg
        self.timeout = timeout

    def start(self):
        seqid = self.msg.reply.split(".")[5]
        task_name = f"MessageProgressUpdater: {id(self)} (seqid={seqid})"
        self._task = asyncio.create_task(self._progress(), name=task_name)

    async def end(self):
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:  # pragma: no cover
            logger.info("MessageProgressUpdater cancelled")
            pass
        except Exception as exc:  # pragma: no cover
            capture_exception(exc)
            logger.exception("Error in MessageProgressUpdater")
            pass

    async def __aenter__(self):
        self.start()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.end()

    async def _progress(self):
        while True:
            try:
                await asyncio.sleep(self.timeout)
                if self.msg._ackd:  # all done, do not mark with in_progress
                    return
                await self.msg.in_progress()
            except (RuntimeError, asyncio.CancelledError):
                return
            except Exception:  # pragma: no cover
                logger.exception("Error sending task progress to NATS")


class NatsConnectionManager:
    _nc: NATSClient
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
    ):
        self._service_name = service_name
        self._nats_servers = nats_servers
        self._nats_creds = nats_creds
        self._subscriptions = []
        self._pull_subscriptions = []
        self._lock = asyncio.Lock()
        self._healthy = True
        self._last_unhealthy: Optional[float] = None

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
            self._nc = await nats.connect(**options)

    async def finalize(self):
        async with self._lock:
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

            # Close the connection
            try:
                await asyncio.wait_for(self._nc.drain(), timeout=1)
            except (
                nats.errors.ConnectionClosedError,
                asyncio.TimeoutError,
            ):  # pragma: no cover
                pass
            await self._nc.close()

    async def disconnected_cb(self) -> None:
        logger.info("Disconnected from NATS!")
        self._last_unhealthy = time.monotonic()

    async def reconnected_cb(self):
        # See who we are connected to on reconnect.
        logger.warning(
            f"Reconnected to NATS {self._nc.connected_url.netloc}. Attempting to re-subscribe."
        )
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

    async def error_cb(self, e):  # pragma: no cover
        logger.error(f"There was an error on consumer: {e}", exc_info=e)

    async def closed_cb(self):  # pragma: no cover
        logger.info("Connection is closed on NATS")

    @property
    def nc(self) -> NATSClient:
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
        psub = await self.js.pull_subscribe(
            subject,
            durable=durable,  # type: ignore
            stream=stream,
            config=config,  # type: ignore
        )

        cancelled = asyncio.Event()

        async def consume(psub: JetStreamContext.PullSubscription, subject: str):
            while True:
                if cancelled.is_set():
                    break
                try:
                    messages = await psub.fetch(batch=1)
                    for message in messages:
                        await cb(message)
                except asyncio.CancelledError:
                    # Handle task cancellation
                    logger.info("Pull subscription consume task cancelled", extra={"subject": subject})
                    break
                except TimeoutError:
                    pass
                except Exception:
                    logger.exception("Error in pull_subscribe task", extra={"subject": subject})

        task = asyncio.create_task(consume(psub, subject), name=f"pull_subscribe_{subject}")
        self._pull_subscriptions.append((psub, task, subscription_lost_cb, cancelled))
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
