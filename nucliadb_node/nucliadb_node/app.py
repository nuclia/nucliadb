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
import signal
import sys
import uuid
from asyncio import tasks
from typing import Callable, List

from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat

from nucliadb_node import SERVICE_NAME, logger
from nucliadb_node.pull import Worker
from nucliadb_node.reader import Reader
from nucliadb_node.sentry import SENTRY, set_sentry
from nucliadb_node.service import start_grpc
from nucliadb_node.settings import settings
from nucliadb_node.writer import Writer
from nucliadb_telemetry.utils import get_telemetry, init_telemetry
from nucliadb_utils.settings import running_settings


class App:
    writer: Writer
    reader: Reader
    worker: Worker
    finalizers: List[Callable]

    def __init__(self):
        self.finalizers = []


async def main() -> App:
    writer = Writer(settings.writer_listen_address)
    reader = Reader(settings.reader_listen_address)

    if settings.force_host_id is None:
        node = None
        i = 0
        while node is None and i < 20:
            try:
                with open(settings.host_key_path, "rb") as file_key:
                    uuid_bytes = file_key.read()
                    node = str(uuid.UUID(bytes=uuid_bytes))
            except FileNotFoundError:
                logger.error("Could not find key")
                node = None
                i += 1
                await asyncio.sleep(2)
    else:
        node = settings.force_host_id

    if node is None:
        raise Exception("No Key defined")

    tracer_provider = get_telemetry(SERVICE_NAME)
    if tracer_provider is not None:
        set_global_textmap(B3MultiFormat())
        await init_telemetry(tracer_provider)  # To start asyncio task

    logger.info(f"Node ID : {node}")

    worker = Worker(writer=writer, reader=reader, node=node)
    await worker.initialize()

    grpc_finalizer = await start_grpc(reader=reader, writer=writer)

    logger.info(f"======= Node sidecar started ======")

    app = App()
    app.worker = worker
    app.writer = writer
    app.reader = reader

    # Using ensure_future as Signal handlers
    # cannot handle couroutines as callbacks
    def stop_pulling():
        logger.info("Received signal to stop pulling!")
        asyncio.ensure_future(worker.finalize())

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGUSR1, stop_pulling)

    app.finalizers = [grpc_finalizer, worker.finalize]

    return app


def _cancel_all_tasks(loop):
    to_cancel = tasks.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    loop.run_until_complete(tasks.gather(*to_cancel, return_exceptions=True))

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler(
                {
                    "message": "unhandled exception during asyncio.run() shutdown",
                    "exception": task.exception(),
                    "task": task,
                }
            )


def run():
    if running_settings.sentry_url and SENTRY:
        set_sentry(
            running_settings.sentry_url,
            running_settings.running_environment,
            running_settings.logging_integration,
        )

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s.%(msecs)02d] [%(levelname)s] - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logger.setLevel(logging.getLevelName(running_settings.log_level.upper()))
    logging.getLogger("asyncio").setLevel(logging.ERROR)

    if asyncio._get_running_loop() is not None:
        raise RuntimeError("cannot be called from a running event loop")

    loop = asyncio.new_event_loop()
    app = None
    try:
        asyncio.set_event_loop(loop)
        if running_settings.debug is not None:
            loop.set_debug(running_settings.debug)
        app = loop.run_until_complete(main())

        loop.run_forever()
    finally:
        try:
            if app is not None:
                for finalizer in app.finalizers:
                    if asyncio.iscoroutinefunction(finalizer):
                        loop.run_until_complete(finalizer())
                    else:
                        finalizer()
            _cancel_all_tasks(loop)
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.run_until_complete(loop.shutdown_default_executor())
        finally:
            asyncio.set_event_loop(None)
            loop.close()
