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

from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import pytest
from fastapi import FastAPI
from uvicorn.server import Server  # type: ignore

from nucliadb_utils.fastapi import run
from nucliadb_utils.settings import running_settings


@pytest.mark.asyncio
async def test_run_server_forever():
    server = AsyncMock(install_signal_handlers=MagicMock(), should_exit=False)
    config = MagicMock(loaded=False)

    await run.run_server_forever(server, config)

    server.main_loop.assert_called_once()
    server.shutdown.assert_called_once()
    server.install_signal_handlers.assert_called_once()


def test_metrics_app():
    server, config = run.metrics_app()

    assert len([r for r in config.app.routes if r.name == "metrics_endpoint"]) == 1
    assert isinstance(server, Server)

    assert config.host == running_settings.metrics_host
    assert config.port == running_settings.metrics_port


@pytest.mark.asyncio
async def test_serve_metrics():
    server = Mock()
    config = Mock()
    with patch("nucliadb_utils.fastapi.run.start_server") as start_server, patch(
        "nucliadb_utils.fastapi.run.metrics_app", return_value=(server, config)
    ):
        await run.serve_metrics()

        start_server.assert_called_once_with(server, config)


def test_run_fastapi_with_metrics():
    app = FastAPI()
    server = AsyncMock(started=True)
    server.config = config = MagicMock()
    metrics_server = AsyncMock(started=True)
    metrics_config = MagicMock()

    with patch(
        "nucliadb_utils.fastapi.run.start_server", AsyncMock()
    ) as start_server, patch(
        "nucliadb_utils.fastapi.run.run_server_forever", AsyncMock()
    ) as run_server_forever, patch(
        "nucliadb_utils.fastapi.run.metrics_app",
        return_value=(metrics_server, metrics_config),
    ), patch(
        "nucliadb_utils.fastapi.run.Server", return_value=server
    ), patch(
        "nucliadb_utils.fastapi.run.Config", return_value=config
    ), patch(
        "nucliadb_utils.fastapi.run.sys.exit", return_value=config
    ) as exit:
        run.run_fastapi_with_metrics(app)

        start_server.assert_called_once_with(metrics_server, metrics_config)
        run_server_forever.assert_called_once_with(server, ANY)
        exit.assert_not_called()

        server.started = False
        run.run_fastapi_with_metrics(app)
        exit.assert_called_once_with(run.STARTUP_FAILURE)
