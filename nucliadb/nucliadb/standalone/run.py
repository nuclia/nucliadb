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
import logging
import os

import pkg_resources
import pydantic_argparse
import uvicorn  # type: ignore
from fastapi import FastAPI

from nucliadb.standalone.config import config_nucliadb
from nucliadb.standalone.settings import Settings
from nucliadb_telemetry import errors
from nucliadb_telemetry.fastapi import instrument_app
from nucliadb_telemetry.logs import setup_logging

logger = logging.getLogger(__name__)


def setup() -> Settings:
    setup_logging()
    errors.setup_error_handling(pkg_resources.get_distribution("nucliadb").version)
    if os.environ.get("NUCLIADB_ENV"):
        nucliadb_args = Settings()
    else:
        parser = pydantic_argparse.ArgumentParser(
            model=Settings,
            prog="NucliaDB",
            description="NucliaDB Starting script",
        )
        nucliadb_args = parser.parse_typed_args()

    config_nucliadb(nucliadb_args)

    return nucliadb_args


def get_server(settings: Settings) -> tuple[FastAPI, uvicorn.Server]:
    from nucliadb.standalone.app import application_factory

    application = application_factory(settings)

    config = uvicorn.Config(
        application, host="0.0.0.0", port=settings.http_port, log_config=None
    )
    server = uvicorn.Server(config)
    config.load()
    server.lifespan = config.lifespan_class(config)
    return application, server


def run():
    settings = setup()
    app, server = get_server(settings)
    instrument_app(app, excluded_urls=["/"], metrics=True)
    logger.warning(
        f"======= Starting server on http://0.0.0.0:{settings.http_port}/ ======"
    )
    server.run()


async def run_async_nucliadb(settings: Settings) -> uvicorn.Server:
    _, server = get_server(settings)
    await server.startup()
    return server


if __name__ == "__main__":
    run()
