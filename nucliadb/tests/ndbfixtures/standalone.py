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
import base64
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import AsyncIterator, Iterator
from unittest.mock import patch

import pytest

from nucliadb.ingest.settings import DriverSettings
from nucliadb.standalone.config import config_nucliadb
from nucliadb.standalone.run import run_async_nucliadb
from nucliadb.standalone.settings import Settings
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.settings import (
    LogFormatType,
    LogLevel,
    LogOutputType,
    LogSettings,
)
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.tests import free_port

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
async def standalone_nucliadb(
    endecryptor_settings,
    dummy_processing,
    analytics_disabled,
    maindb_settings: DriverSettings,
    storage: Storage,
    storage_settings,
    tmp_path: Path,
    learning_config,
) -> AsyncIterator[Settings]:
    # we need to force DATA_PATH updates to run every test on the proper
    # temporary directory
    data_path = str((tmp_path / "node").absolute())
    with (
        safe_global_config(),
        patch.dict(os.environ, {"DATA_PATH": data_path}, clear=False),
    ):
        settings = Settings(
            data_path=data_path,
            http_port=free_port(),
            ingest_grpc_port=free_port(),
            train_grpc_port=free_port(),
            log_format_type=LogFormatType.PLAIN,
            log_output_type=LogOutputType.FILE,
            **maindb_settings.model_dump(),
            **storage_settings,
        )

        config_nucliadb(settings)

        # Make sure tests don't write logs outside of the tmpdir
        os.environ["ERROR_LOG"] = str((tmp_path / "logs" / "error.log").absolute())
        os.environ["ACCESS_LOG"] = str((tmp_path / "logs" / "access.log").absolute())
        os.environ["INFO_LOG"] = str((tmp_path / "logs" / "info.log").absolute())

        setup_logging(
            settings=LogSettings(
                log_output_type=LogOutputType.FILE,
                log_format_type=LogFormatType.PLAIN,
                debug=False,
                log_level=LogLevel.WARNING,
            )
        )
        server = await run_async_nucliadb(settings)
        assert server.started, "Nucliadb server did not start correctly"

        yield settings

        await server.shutdown()


# Old fixture name for bw/c while migrating. TODO: remove once everything has been migrated
@pytest.fixture(scope="function")
def nucliadb(standalone_nucliadb: Settings) -> Iterator[Settings]:
    yield standalone_nucliadb


@pytest.fixture(scope="function", autouse=True)
def analytics_disabled():
    with patch.dict(
        os.environ,
        {
            "NUCLIADB_DISABLE_ANALYTICS": "True",
        },
        clear=False,
    ):
        yield


@pytest.fixture(scope="function")
def endecryptor_settings():
    from nucliadb_utils.encryption.settings import settings

    secret_key = os.urandom(32)
    encoded_secret_key = base64.b64encode(secret_key).decode("utf-8")

    with patch.object(settings, "encryption_secret_key", encoded_secret_key):
        yield


@contextmanager
def safe_global_config():
    """Save a copy of nucliadb global state (across all settings) and restore it
    afterwards.

    """
    import nucliadb.backups.settings
    import nucliadb.common.back_pressure.settings
    import nucliadb.common.cluster.settings
    import nucliadb.common.external_index_providers.settings
    import nucliadb.ingest.settings
    import nucliadb.migrator.settings
    import nucliadb.search.settings
    import nucliadb.standalone.settings
    import nucliadb.train.settings
    import nucliadb.writer.settings
    import nucliadb_telemetry.settings
    import nucliadb_utils.cache.settings
    import nucliadb_utils.encryption.settings
    import nucliadb_utils.settings
    import nucliadb_utils.storages.settings

    all_settings = [
        nucliadb.backups.settings.settings,
        nucliadb.common.back_pressure.settings.settings,
        nucliadb.common.cluster.settings.settings,
        nucliadb.common.external_index_providers.settings.settings,
        nucliadb.ingest.settings.settings,
        nucliadb.migrator.settings.settings,
        nucliadb.search.settings.settings,
        nucliadb.train.settings.settings,
        nucliadb.writer.settings.settings,
        nucliadb_telemetry.settings.telemetry_settings,
        nucliadb_utils.cache.settings.settings,
        nucliadb_utils.encryption.settings.settings,
        nucliadb_utils.settings.audit_settings,
        nucliadb_utils.settings.http_settings,
        nucliadb_utils.settings.indexing_settings,
        nucliadb_utils.settings.nuclia_settings,
        nucliadb_utils.settings.nucliadb_settings,
        nucliadb_utils.settings.storage_settings,
        nucliadb_utils.settings.transaction_settings,
        nucliadb_utils.storages.settings,
    ]

    global_state = []
    # save current global state
    for settings in all_settings:
        for attr, value in settings:
            global_state.append((settings, attr, value))

    yield

    # restore previous state
    for settings, attr, value in global_state:
        setattr(settings, attr, value)
