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
import sys
from asyncio import tasks
from typing import Callable, List

from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.b3 import B3MultiFormat

from nucliadb.sentry import SENTRY, set_sentry
from nucliadb.train import SERVICE_NAME, logger
from nucliadb.train.server import start_grpc
from nucliadb_telemetry.utils import get_telemetry, init_telemetry
from nucliadb_utils.settings import running_settings


async def main() -> List[Callable]:
    tracer_provider = get_telemetry(SERVICE_NAME)
    if tracer_provider is not None:
        set_global_textmap(B3MultiFormat())
        await init_telemetry(tracer_provider)  # To start asyncio task

    grpc_finalizer = await start_grpc(SERVICE_NAME)
    logger.info(f"======= Train finished starting ======")
    finalizers = [
        grpc_finalizer,
    ]

    return finalizers


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
        format="%(asctime)-18s | %(levelname)-7s | %(name)-16s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logger.setLevel(logging.getLevelName(running_settings.log_level.upper()))

    logging.getLogger("asyncio").setLevel(logging.ERROR)

    if asyncio._get_running_loop() is not None:
        raise RuntimeError("cannot be called from a running event loop")

    loop = asyncio.new_event_loop()
    finalizers: List[Callable] = []
    try:
        asyncio.set_event_loop(loop)
        if running_settings.debug is not None:
            loop.set_debug(running_settings.debug)
        finalizers.extend(loop.run_until_complete(main()))

        loop.run_forever()
    finally:
        try:
            for finalizer in finalizers:
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
