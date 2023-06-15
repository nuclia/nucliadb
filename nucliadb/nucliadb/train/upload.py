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
import argparse
import asyncio
from asyncio import tasks
from typing import Callable, List

import pkg_resources

from nucliadb.train.uploader import start_upload
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging
from nucliadb_utils.settings import running_settings


def arg_parse():
    parser = argparse.ArgumentParser(description="Upload data to Nuclia Learning API.")

    parser.add_argument(
        "-r", "--request", dest="request", help="Request UUID", required=True
    )

    parser.add_argument("-k", "--kb", dest="kb", help="Knowledge Box", required=True)


async def main() -> List[Callable]:
    parser = arg_parse()

    await start_upload(parser.request, parser.kb)
    finalizers: List[Callable] = []

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


def run() -> None:
    setup_logging()

    errors.setup_error_handling(pkg_resources.get_distribution("nucliadb").version)

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
