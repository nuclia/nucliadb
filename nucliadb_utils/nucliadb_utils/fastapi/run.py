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

import click
from uvicorn.config import Config  # type: ignore
from uvicorn.server import Server  # type: ignore

from nucliadb_utils import logger
from nucliadb_utils.fastapi.metrics import application_metrics
from nucliadb_utils.settings_running import running_settings

STARTUP_FAILURE = 3


def run_fastapi_with_metrics(application):

    loop_setup = "auto"
    log_level = running_settings.log_level.lower()
    if log_level == "warn":
        log_level = "warning"

    metrics_config = Config(
        application_metrics,
        host=running_settings.metrics_host,
        port=running_settings.metrics_port,
        debug=running_settings.debug,
        loop=loop_setup,
        http="auto",
        reload=False,
        workers=1,
        use_colors=False,
        log_level=log_level,
        limit_concurrency=None,
        backlog=2047,
        limit_max_requests=None,
        timeout_keep_alive=5,
    )
    metrics_server = Server(config=metrics_config)

    config = Config(
        application,
        host=running_settings.serving_host,
        port=running_settings.serving_port,
        debug=running_settings.debug,
        loop=loop_setup,
        http="auto",
        reload=False,
        workers=1,
        use_colors=False,
        log_level=log_level,
        limit_concurrency=None,
        backlog=2047,
        limit_max_requests=None,
        timeout_keep_alive=5,
    )
    server = Server(config=config)

    server.config.setup_event_loop()
    asyncio.run(
        serve(
            main_server=server,
            main_config=config,
            metrics_server=metrics_server,
            metrics_config=metrics_config,
        )
    )

    if not metrics_server.started or not server.started:
        sys.exit(STARTUP_FAILURE)


async def serve(
    main_server: Server,
    main_config: Config,
    metrics_server: Server,
    metrics_config: Config,
):
    process_id = os.getpid()

    if not main_config.loaded:
        main_config.load()

    if not metrics_config.loaded:
        metrics_config.load()

    main_server.lifespan = main_config.lifespan_class(main_config)
    metrics_server.lifespan = metrics_config.lifespan_class(metrics_config)

    main_server.install_signal_handlers()

    message = "Started server process [%d]"
    color_message = "Started server process [" + click.style("%d", fg="cyan") + "]"
    logger.info(message, process_id, extra={"color_message": color_message})

    await main_server.startup()
    await metrics_server.startup()
    if main_server.should_exit:
        return
    await main_server.main_loop()
    await main_server.shutdown()
    await metrics_server.shutdown()

    message = "Finished server process [%d]"
    color_message = "Finished server process [" + click.style("%d", fg="cyan") + "]"
    logger.info(message, process_id, extra={"color_message": color_message})
