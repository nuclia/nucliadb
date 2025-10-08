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
from os.path import dirname
from typing import AsyncIterable, AsyncIterator, Iterator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pytest_mock import MockerFixture

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.maindb.driver import Driver
from nucliadb.search.predict import DummyPredictEngine
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.audit.basic import BasicAuditStorage
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.cache.nats import NatsPubsub
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.settings import (
    audit_settings,
    nuclia_settings,
)
from nucliadb_utils.storages.settings import settings as storage_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import (
    Utility,
    get_utility,
)
from tests.ndbfixtures.utils import global_utility

# Audit


@pytest.fixture(scope="function")
def audit(basic_audit: BasicAuditStorage) -> Iterator[AuditStorage]:
    yield basic_audit


@pytest.fixture(scope="function")
async def basic_audit() -> AsyncIterator[BasicAuditStorage]:
    audit = BasicAuditStorage()
    await audit.initialize()
    with global_utility(Utility.AUDIT, audit):
        yield audit
    await audit.finalize()


@pytest.fixture(scope="function")
async def stream_audit(nats_server: str, mocker: MockerFixture) -> AsyncIterator[StreamAuditStorage]:
    audit = StreamAuditStorage(
        [nats_server],
        audit_settings.audit_jetstream_target,  # type: ignore
        audit_settings.audit_partitions,
        audit_settings.audit_hash_seed,
    )
    await audit.initialize()

    mocker.spy(audit, "send")
    mocker.spy(audit.js, "publish")
    mocker.spy(audit, "search")
    mocker.spy(audit, "chat")

    with global_utility(Utility.AUDIT, audit):
        yield audit

    await audit.finalize()


# Local files


@pytest.fixture(scope="function")
async def local_files():
    storage_settings.local_testing_files = f"{dirname(__file__)}"


# Predict


@pytest.fixture(scope="function")
def predict_mock() -> Mock:  # type: ignore
    mock = AsyncMock()

    with global_utility(Utility.PREDICT, mock):
        yield mock


@pytest.fixture(scope="function")
async def dummy_predict() -> AsyncIterable[DummyPredictEngine]:
    with (
        patch.object(nuclia_settings, "dummy_predict", True),
    ):
        predict_util = DummyPredictEngine()
        await predict_util.initialize()

        with global_utility(Utility.PREDICT, predict_util):
            yield predict_util


# PubSub


@pytest.fixture(scope="function")
async def pubsub(nats_server: str) -> AsyncIterator[PubSubDriver]:
    assert get_utility(Utility.PUBSUB) is None, "No pubsub is expected to be already set"

    pubsub = NatsPubsub(hosts=[nats_server])
    await pubsub.initialize()
    with global_utility(Utility.PUBSUB, pubsub):
        yield pubsub
    await pubsub.finalize()


# Shard manager


@pytest.fixture(scope="function")
async def shard_manager(storage: Storage, maindb_driver: Driver) -> AsyncIterator[KBShardManager]:
    sm = KBShardManager()
    with global_utility(Utility.SHARD_MANAGER, sm):
        yield sm
