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
import os
import sys
from typing import Optional

import argdantic
import uvicorn  # type: ignore
from fastapi import FastAPI

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb.standalone import versions
from nucliadb.standalone.config import config_nucliadb
from nucliadb.standalone.migrations import run_migrations
from nucliadb.standalone.settings import Settings
from nucliadb_telemetry import errors
from nucliadb_telemetry.fastapi import instrument_app
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.settings import LogOutputType, LogSettings
from nucliadb_utils.settings import nuclia_settings, storage_settings

logger = logging.getLogger(__name__)


parser: argdantic.ArgParser = argdantic.ArgParser()


@parser.command(singleton=True, name="NucliaDB", help="NucliaDB Starting script")
def setting_parser(settings: Settings) -> Settings:
    return settings


def setup() -> Settings:
    errors.setup_error_handling(versions.get_installed_version("nucliadb"))
    nucliadb_args = parser()

    log_settings = LogSettings(
        # change default settings for standalone
        log_output_type=nucliadb_args.log_output_type,
        log_format_type=nucliadb_args.log_format_type,
        log_level=nucliadb_args.log_level,
    )
    setup_logging(settings=log_settings)

    config_nucliadb(nucliadb_args)

    return nucliadb_args


def get_server(settings: Settings) -> tuple[FastAPI, uvicorn.Server]:
    from nucliadb.standalone.app import application_factory

    application = application_factory(settings)

    config = uvicorn.Config(
        application, host=settings.http_host, port=settings.http_port, log_config=None
    )
    server = uvicorn.Server(config)
    config.load()
    server.lifespan = config.lifespan_class(config)

    return application, server


def run():
    settings = setup()
    run_migrations()
    app, server = get_server(settings)
    instrument_app(app, excluded_urls=["/"], metrics=True)

    if settings.fork:  # pragma: no cover
        pid = os.fork()
        if pid != 0:
            sys.stdout.write(f"Server forked and running on pid {pid}.")
            return

    settings_to_output = {
        "API": f"http://{settings.http_host}:{settings.http_port}/api",
        "Admin UI": f"http://{settings.http_host}:{settings.http_port}/admin",
        "Key-value backend": ingest_settings.driver.value,
        "Blob storage backend": storage_settings.file_backend.value,
        "Index data path": os.path.realpath(cluster_settings.data_path),
        "Auth policy": settings.auth_policy.value,
        "Node role": cluster_settings.standalone_node_role.value,
    }
    log_settings = {
        "Log output type": settings.log_output_type.value,
        "Log format type": settings.log_format_type.value,
        "Log level": settings.log_level.value,
    }
    if settings.log_output_type == LogOutputType.FILE:
        log_folder = os.path.realpath(os.path.dirname(LogSettings().access_log))
        log_settings["Log folder path"] = log_folder
    settings_to_output.update(log_settings)

    if nuclia_settings.nuclia_service_account:
        settings_to_output["NUA API key"] = "Configured ✔"
        settings_to_output["NUA API zone"] = nuclia_settings.nuclia_zone

    settings_to_output_fmted = "\n".join(
        [f"||      - {k}:{' ' * (27 - len(k))}{v}" for k, v in settings_to_output.items()]
    )

    installed_version = versions.installed_nucliadb()
    latest_version = get_latest_nucliadb()
    if latest_version is None:
        version_info_fmted = f"{installed_version} (Update check failed)"
    elif versions.nucliadb_updates_available(installed_version, latest_version):
        version_info_fmted = f"{installed_version} (Update available: {latest_version})"
    else:
        version_info_fmted = installed_version

    sys.stdout.write(
        f"""=================================================
||
||   NucliaDB Standalone Server Running!
||
||   Version: {version_info_fmted}
||
||   Configuration:
{settings_to_output_fmted}
=================================================
"""
    )
    server.run()


def get_latest_nucliadb() -> Optional[str]:
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(versions.latest_nucliadb())


async def run_async_nucliadb(settings: Settings) -> uvicorn.Server:
    _, server = get_server(settings)
    await server.startup()
    return server


if __name__ == "__main__":
    run()
