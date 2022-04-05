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
import sys

from grpc import aio  # type: ignore
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb_utils.partition import PartitionUtility
from nucliadb_utils.settings import (
    nuclia_settings,
    nucliadb_settings,
    running_settings,
    storage_settings,
    transaction_settings,
)
from nucliadb_utils.transaction import TransactionUtility
from nucliadb_utils.utilities import (
    Utility,
    clean_utility,
    get_transaction,
    get_utility,
    set_utility,
)
from nucliadb_writer import logger
from nucliadb_writer.processing import ProcessingEngine
from nucliadb_writer.tus import finalize as storage_finalize
from nucliadb_writer.tus import initialize as storage_initialize
from nucliadb_writer.utilities import get_processing


async def initialize():
    set_utility(
        Utility.CHANNEL, aio.insecure_channel(nucliadb_settings.nucliadb_ingest)
    )
    set_utility(Utility.INGEST, WriterStub(get_utility(Utility.CHANNEL)))
    processing_engine = ProcessingEngine(
        nuclia_service_account=nuclia_settings.nuclia_service_account,
        nuclia_zone=nuclia_settings.nuclia_zone,
        onprem=nuclia_settings.onprem,
        nuclia_jwt_key=nuclia_settings.nuclia_jwt_key,
        nuclia_proxy_cluster_url=nuclia_settings.nuclia_proxy_cluster_url,
        nuclia_proxy_public_url=nuclia_settings.nuclia_proxy_public_url,
        driver=storage_settings.file_backend,
        dummy=nuclia_settings.dummy_processing,
    )
    await processing_engine.initialize()
    set_utility(Utility.PROCESSING, processing_engine)
    set_utility(
        Utility.PARTITION,
        PartitionUtility(
            partitions=nuclia_settings.nuclia_partitions,
            seed=nuclia_settings.nuclia_hash_seed,
        ),
    )
    transaction_utility = TransactionUtility(
        nats_creds=transaction_settings.transaction_jetstream_auth,
        nats_servers=transaction_settings.transaction_jetstream_servers,
        nats_target=transaction_settings.transaction_jetstream_target,
    )
    await transaction_utility.initialize()
    set_utility(Utility.TRANSACTION, transaction_utility)
    await storage_initialize()

    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s.%(msecs)02d] [%(levelname)s] - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )

    logger.setLevel(logging.getLevelName(running_settings.log_level.upper()))


async def finalize():
    transaction = get_transaction()
    if transaction is not None:
        await transaction.finalize()

    if get_utility(Utility.CHANNEL):
        await get_utility(Utility.CHANNEL).close()
        clean_utility(Utility.CHANNEL)
        clean_utility(Utility.INGEST)
    processing = get_processing()
    if processing is not None:
        await processing.finalize()

    await storage_finalize()
