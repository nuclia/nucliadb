# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

from fastapi import FastAPI
from uvicorn.server import Server

from nucliadb_utils.fastapi import run
from nucliadb_utils.settings import running_settings


async def test_run_server_forever():
    server = AsyncMock(install_signal_handlers=MagicMock(), should_exit=False)
    config = MagicMock(loaded=False)

    await run.run_server_forever(server, config)

    server.main_loop.assert_called_once()
    server.shutdown.assert_called_once()
    server.install_signal_handlers.assert_called_once()


def test_metrics_app():
    server, config = run.metrics_app()

    assert len([r for r in config.app.routes if r.name == "metrics_endpoint"]) == 1  # type: ignore[ty:unresolved-attribute]
    assert isinstance(server, Server)

    assert config.host == running_settings.metrics_host
    assert config.port == running_settings.metrics_port


async def test_serve_metrics():
    server = Mock()
    config = Mock()
    with (
        patch("nucliadb_utils.fastapi.run.start_server") as start_server,
        patch("nucliadb_utils.fastapi.run.metrics_app", return_value=(server, config)),
    ):
        await run.serve_metrics()

        start_server.assert_called_once_with(server, config)


def test_run_fastapi_with_metrics():
    app = FastAPI()
    server = AsyncMock(started=True)
    server.config = config = MagicMock()
    metrics_server = AsyncMock(started=True)
    metrics_config = MagicMock()

    with (
        patch("nucliadb_utils.fastapi.run.start_server", AsyncMock()) as start_server,
        patch("nucliadb_utils.fastapi.run.run_server_forever", AsyncMock()) as run_server_forever,
        patch(
            "nucliadb_utils.fastapi.run.metrics_app",
            return_value=(metrics_server, metrics_config),
        ),
        patch("nucliadb_utils.fastapi.run.Server", return_value=server),
        patch("nucliadb_utils.fastapi.run.Config", return_value=config),
        patch("nucliadb_utils.fastapi.run.sys.exit", return_value=config) as exit,
    ):
        run.run_fastapi_with_metrics(app)

        start_server.assert_called_once_with(metrics_server, metrics_config)
        run_server_forever.assert_called_once_with(server, ANY)
        exit.assert_not_called()

        server.started = False
        run.run_fastapi_with_metrics(app)
        exit.assert_called_once_with(run.STARTUP_FAILURE)
