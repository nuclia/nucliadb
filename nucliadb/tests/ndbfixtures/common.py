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
from typing import AsyncIterator, Iterator

import nats
import pytest

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.maindb.driver import Driver
from nucliadb_utils.audit.basic import BasicAuditStorage
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.cache.nats import NatsPubsub
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.indexing import IndexingUtility
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import audit_settings, transaction_settings
from nucliadb_utils.storages.settings import settings as storage_settings
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.transaction import TransactionUtility
from nucliadb_utils.utilities import (
    Utility,
    get_utility,
    start_nats_manager,
    start_transaction_utility,
    stop_nats_manager,
    stop_transaction_utility,
)
from tests.ndbfixtures.utils import global_utility


# XXX: renamed as ingest overwrites this and call it from a loot of tests
@pytest.fixture(scope="function")
async def local_files__ndbfixtures():
    storage_settings.local_testing_files = f"{dirname(__file__)}"


# Audit


@pytest.fixture(scope="function")
def audit(basic_audit: BasicAuditStorage) -> Iterator[BasicAuditStorage]:
    yield basic_audit


@pytest.fixture(scope="function")
async def basic_audit() -> AsyncIterator[BasicAuditStorage]:
    audit = BasicAuditStorage()
    await audit.initialize()
    yield audit
    await audit.finalize()


@pytest.fixture(scope="function")
async def stream_audit(nats_server: str) -> AsyncIterator[StreamAuditStorage]:
    audit = StreamAuditStorage(
        [nats_server],
        audit_settings.audit_jetstream_target,  # type: ignore
        audit_settings.audit_partitions,
        audit_settings.audit_hash_seed,
    )
    await audit.initialize()
    yield audit
    await audit.finalize()


# Indexing utility


@pytest.fixture(scope="function")
async def indexing_utility():
    """Dummy indexing utility. As it's a dummy, we don't need to provide real
    nats servers or creds. Ideally, we should have a different utility instead
    of playing with a parameter.

    """
    indexing_utility = IndexingUtility(nats_creds=None, nats_servers=[], dummy=True)
    await indexing_utility.initialize()
    with global_utility(Utility.INDEXING, indexing_utility):
        yield
    await indexing_utility.finalize()


# Nats


@pytest.fixture(scope="function")
async def nats_server(natsd: str):
    yield natsd

    # cleanup nats
    nc = await nats.connect(servers=[natsd])
    await nc.drain()
    await nc.close()


@pytest.fixture(scope="function")
async def nats_manager(nats_server: str) -> AsyncIterator[NatsConnectionManager]:
    ncm = await start_nats_manager("nucliadb_tests", [nats_server], None)
    yield ncm
    await stop_nats_manager()


# Pubsub


@pytest.fixture(scope="function")
def pubsub(nats_pubsub: NatsPubsub) -> Iterator[PubSubDriver]:
    pubsub = get_utility(Utility.PUBSUB)
    assert pubsub is None, "No pubsub is expected to be here"

    with global_utility(Utility.PUBSUB, nats_pubsub):
        yield nats_pubsub


@pytest.fixture(scope="function")
async def nats_pubsub(nats_server: str) -> AsyncIterator[NatsPubsub]:
    pubsub = NatsPubsub(hosts=[nats_server])
    await pubsub.initialize()

    yield pubsub

    await pubsub.finalize()


# Shard manager (index)


@pytest.fixture(scope="function")
async def shard_manager(storage: Storage, maindb_driver: Driver) -> AsyncIterator[KBShardManager]:
    sm = KBShardManager()
    with global_utility(Utility.SHARD_MANAGER, sm):
        yield sm


# Transaction utility


@pytest.fixture(scope="function")
def transaction_utility(nats_transaction_utility: TransactionUtility) -> Iterator[TransactionUtility]:
    yield nats_transaction_utility


@pytest.fixture(scope="function")
async def nats_transaction_utility(
    nats_server: str, pubsub: PubSubDriver
) -> AsyncIterator[TransactionUtility]:
    """NATS transaction utility is used when components are phisically
    distributed.

    """
    transaction_settings.transaction_jetstream_servers = [nats_server]
    util = await start_transaction_utility()
    yield util
    await stop_transaction_utility()
