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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nucliadb_sidecar import app

pytestmark = pytest.mark.asyncio


async def test_main():
    with (
        patch("nucliadb_sidecar.app.get_storage", AsyncMock()) as storage,
        patch("nucliadb_sidecar.app.start_worker", AsyncMock()) as start_worker,
        patch("nucliadb_sidecar.app.start_nats_manager", AsyncMock()) as _,
        patch("nucliadb_sidecar.app.stop_nats_manager", AsyncMock()) as stop_nats_manager,
        patch("nucliadb_sidecar.app.start_indexed_publisher", AsyncMock()) as start_indexed_publisher,
        patch("nucliadb_sidecar.app.start_grpc", AsyncMock()) as start_grpc,
        patch("nucliadb_sidecar.app.serve_metrics", AsyncMock()) as serve_metrics,
        patch("nucliadb_sidecar.app.run_until_exit", AsyncMock()) as run_until_exit,
        patch("nucliadb_sidecar.app.Writer", MagicMock()) as writer,
        patch(
            "nucliadb_sidecar.app.get_storage",
            AsyncMock(),
        ) as storage,
    ):
        await app.main()

        run_until_exit.assert_awaited_once_with(
            [
                start_grpc.return_value,
                start_worker.return_value.finalize,
                start_indexed_publisher.return_value.finalize,
                serve_metrics.return_value.shutdown,
                writer.return_value.close,
                stop_nats_manager,
                storage.return_value.finalize,
            ]
        )
