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
from nucliadb.ingest.processing import start_processing_engine
from nucliadb.ingest.utils import start_ingest, stop_ingest
from nucliadb.writer import SERVICE_NAME
from nucliadb.writer.tus import finalize as storage_finalize
from nucliadb.writer.tus import initialize as storage_initialize
from nucliadb.writer.utilities import get_processing
from nucliadb_telemetry.utils import clean_telemetry, setup_telemetry
from nucliadb_utils.partition import PartitionUtility
from nucliadb_utils.settings import nuclia_settings, transaction_settings
from nucliadb_utils.transaction import LocalTransactionUtility, TransactionUtility
from nucliadb_utils.utilities import (
    Utility,
    finalize_utilities,
    get_transaction_utility,
    set_utility,
)


async def initialize():
    await setup_telemetry(SERVICE_NAME)

    await start_ingest(SERVICE_NAME)

    await start_processing_engine()

    set_utility(
        Utility.PARTITION,
        PartitionUtility(
            partitions=nuclia_settings.nuclia_partitions,
            seed=nuclia_settings.nuclia_hash_seed,
        ),
    )
    if transaction_settings.transaction_local:
        transaction_utility = LocalTransactionUtility()
    else:
        transaction_utility = TransactionUtility(
            nats_creds=transaction_settings.transaction_jetstream_auth,
            nats_servers=transaction_settings.transaction_jetstream_servers,
            nats_target=transaction_settings.transaction_jetstream_target,
        )
        await transaction_utility.initialize(SERVICE_NAME)
    set_utility(Utility.TRANSACTION, transaction_utility)
    await storage_initialize()


async def finalize():
    transaction = get_transaction_utility()
    if transaction is not None:
        await transaction.finalize()

    await stop_ingest()
    processing = get_processing()
    if processing is not None:
        await processing.finalize()

    await storage_finalize()

    await clean_telemetry(SERVICE_NAME)

    await finalize_utilities()
