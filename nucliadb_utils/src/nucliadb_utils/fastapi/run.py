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
import sys
from contextlib import AbstractContextManager, nullcontext

import click
from fastapi import FastAPI
from uvicorn.config import Config  # type: ignore
from uvicorn.server import Server  # type: ignore

from nucliadb_telemetry.fastapi import application_metrics
from nucliadb_utils import logger
from nucliadb_utils.settings import running_settings

STARTUP_FAILURE = 3


def metrics_app() -> tuple[Server, Config]:
    metrics_config = Config(
        application_metrics,
        host=running_settings.metrics_host,
        port=running_settings.metrics_port,
        loop="auto",
        http="auto",
        reload=False,
        workers=1,
        use_colors=False,
        log_config=None,
        limit_concurrency=None,
        backlog=2047,
        limit_max_requests=None,
        timeout_keep_alive=5,
    )
    metrics_server = Server(config=metrics_config)
    return metrics_server, metrics_config


async def serve_metrics() -> Server:
    server, config = metrics_app()
    await start_server(server, config)
    return server


def run_fastapi_with_metrics(application: FastAPI) -> None:
    metrics_server, metrics_config = metrics_app()
    config = Config(
        application,
        host=running_settings.serving_host,
        port=running_settings.serving_port,
        loop="auto",
        http="auto",
        reload=False,
        workers=1,
        use_colors=False,
        log_config=None,
        limit_concurrency=None,
        backlog=2047,
        limit_max_requests=None,
        timeout_keep_alive=5,
    )
    server = Server(config=config)

    server.config.setup_event_loop()

    async def serve_both():
        await start_server(metrics_server, metrics_config)
        await run_server_forever(server, config)
        await metrics_server.shutdown()

    asyncio.run(serve_both())

    if not metrics_server.started or not server.started:
        sys.exit(STARTUP_FAILURE)


async def start_server(server: Server, config: Config):
    """
    Abstracted out of Server.serve to allow running multiple servers
    and not trounce on signal handlers.
    """
    if not config.loaded:
        config.load()

    server.lifespan = config.lifespan_class(config)

    await server.startup()


async def run_server_forever(server: Server, config: Config):
    await start_server(server, config)
    process_id = os.getpid()

    # compatibility with uvicorn<0.29
    capture_signals: AbstractContextManager
    if hasattr(server, "install_signal_handlers"):  # pragma: no cover
        server.install_signal_handlers()
        capture_signals = nullcontext()
    else:
        # uvicorn>=0.29
        capture_signals = server.capture_signals()

    with capture_signals:
        message = "Started server process [%d]"
        color_message = "Started server process [" + click.style("%d", fg="cyan") + "]"
        logger.info(message, process_id, extra={"color_message": color_message})

        if server.should_exit:
            return

        await server.main_loop()
        await server.shutdown()
