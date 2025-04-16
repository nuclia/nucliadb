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
from pathlib import Path
from unittest.mock import patch

import pytest

from nucliadb.common.maindb.exceptions import UnsetUtility
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.settings import DriverSettings
from nucliadb.standalone.config import config_nucliadb
from nucliadb.standalone.run import get_server
from nucliadb.standalone.settings import Settings
from nucliadb.tests.config import reset_config
from nucliadb_telemetry.fastapi import instrument_app
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.settings import (
    LogFormatType,
    LogLevel,
    LogOutputType,
    LogSettings,
)
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.tests import free_port
from tests.ndbfixtures import SERVICE_NAME

from .maindb import cleanup_maindb

logger = logging.getLogger(__name__)


@pytest.fixture(scope="function")
async def nucliadb(
    endecryptor_settings,
    dummy_processing,
    analytics_disabled,
    maindb_settings: DriverSettings,
    storage: Storage,
    storage_settings,
    tmp_path: Path,
    learning_config,
):
    # we need to force DATA_PATH updates to run every test on the proper
    # temporary directory
    data_path = str((tmp_path / "node").absolute())
    with patch.dict(os.environ, {"DATA_PATH": data_path}, clear=False):
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

        application, server = get_server(settings)
        instrument_app(
            application,
            tracer_provider=get_telemetry(SERVICE_NAME),
            excluded_urls=["/"],
            metrics=True,
            trace_id_on_responses=True,
        )
        await server.startup()
        assert server.started, "Nucliadb server did not start correctly"

        yield settings

        await maybe_cleanup_maindb()

        reset_config()
        await server.shutdown()


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


# Utils


async def maybe_cleanup_maindb():
    try:
        driver = get_driver()
    except UnsetUtility:
        pass
    else:
        try:
            await cleanup_maindb(driver)
        except Exception:
            logger.error("Could not cleanup maindb on test teardown")
            pass
