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
from aiohttp import web

from nucliadb.ingest import logger

try:
    import prometheus_client  # type: ignore

    PROMETHEUS = True
except ImportError:
    PROMETHEUS = False


async def handler(request):
    if PROMETHEUS:
        output = prometheus_client.exposition.generate_latest()
        return output.decode("utf8")
    else:
        return web.Response(text=f"OK")


class MetricsService:
    def __init__(self, host: str = "0.0.0.0", port: int = 8081, debug: bool = False):
        self.port = port
        self.host = host
        self.debug = debug
        self.site = None

    async def start(self):
        server = web.Server(handler)
        runner = web.ServerRunner(server)
        await runner.setup()
        self.site = web.TCPSite(runner, self.host, self.port)
        await self.site.start()
        logger.info(
            f"======= Ingest metrics serving on http://{self.host}:{self.port}/ ======"
        )

    async def stop(self):
        try:
            await self.site.stop()
        except RuntimeError:
            # pytest bug makes this be called twice in the same test teardown:
            # https://github.com/aio-libs/aiohttp/issues/4684
            pass
