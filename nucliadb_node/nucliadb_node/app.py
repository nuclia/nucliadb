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

import pkg_resources

from nucliadb_node import SERVICE_NAME, logger
from nucliadb_node.pull import Worker
from nucliadb_node.service import start_grpc
from nucliadb_node.settings import settings
from nucliadb_node.writer import Writer
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.fastapi.run import serve_metrics
from nucliadb_utils.run import run_until_exit


async def start_worker(writer: Writer) -> Worker:
    if settings.force_host_id is None:  # pragma: no cover
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

    worker = Worker(writer=writer, node=node)
    await worker.initialize()

    return worker


async def main():
    writer = Writer(settings.writer_listen_address)
    worker = await start_worker(writer)

    await setup_telemetry(SERVICE_NAME)

    logger.info(f"Node ID : {worker.node}")

    grpc_finalizer = await start_grpc(writer=writer)

    logger.info(f"======= Node sidecar started ======")

    metrics_server = await serve_metrics()

    finalizers = [
        grpc_finalizer,
        worker.finalize,
        metrics_server.shutdown,
        writer.close,
    ]

    await run_until_exit(finalizers)


def run():  # pragma: no cover
    setup_logging()

    errors.setup_error_handling(pkg_resources.get_distribution("nucliadb_node").version)

    asyncio.run(main())
