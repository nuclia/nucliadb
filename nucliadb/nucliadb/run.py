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

import pydantic_argparse
import uvicorn  # type: ignore
from fastapi.staticfiles import StaticFiles

from nucliadb.config import config_nucliadb
from nucliadb.settings import Settings
from nucliadb_telemetry.logs import setup_logging

logger = logging.getLogger(__name__)


def run():
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
    run_nucliadb(nucliadb_args)


def run_nucliadb(nucliadb_args: Settings):
    setup_logging()

    from nucliadb.one.app import application

    path = os.path.dirname(__file__) + "/static"
    application.mount("/widget", StaticFiles(directory=path, html=True), name="widget")
    logger.warning(
        f"======= Starting server on http://0.0.0.0:{nucliadb_args.http_port}/ ======"
    )
    uvicorn.run(
        application,
        host="0.0.0.0",
        port=nucliadb_args.http_port,
        log_config=None,
        debug=True,
        reload=False,
    )


async def run_async_nucliadb(nucliadb_args: Settings):
    from nucliadb.one.app import application

    config = uvicorn.Config(application, port=nucliadb_args.http_port, log_config=None)
    server = uvicorn.Server(config)
    config.load()
    server.lifespan = config.lifespan_class(config)
    await server.startup()
    return server


if __name__ == "__main__":
    nucliadb_args = Settings()
    config_nucliadb(nucliadb_args)
    run_nucliadb(nucliadb_args)
