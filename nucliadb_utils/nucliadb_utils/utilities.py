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

from __future__ import annotations

import asyncio
import hashlib
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum
from typing import TYPE_CHECKING, Any, List, Optional, Union, cast

from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb_utils import featureflagging
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.audit.basic import BasicAuditStorage
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.cache.nats import NatsPubsub
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.cache.settings import settings as cache_settings
from nucliadb_utils.exceptions import ConfigurationError
from nucliadb_utils.indexing import IndexingUtility
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.partition import PartitionUtility
from nucliadb_utils.settings import (
    FileBackendConfig,
    audit_settings,
    indexing_settings,
    nuclia_settings,
    storage_settings,
    transaction_settings,
)
from nucliadb_utils.storages.settings import settings as extended_storage_settings
from nucliadb_utils.store import MAIN

if TYPE_CHECKING:  # pragma: no cover
    from nucliadb_utils.storages.local import LocalStorage
    from nucliadb_utils.storages.nuclia import NucliaStorage
    from nucliadb_utils.storages.storage import Storage
    from nucliadb_utils.transaction import TransactionUtility

logger = logging.getLogger(__name__)


class Utility(str, Enum):
    INGEST = "ingest"
    CHANNEL = "channel"
    PARTITION = "partition"
    PREDICT = "predict"
    PROCESSING = "processing"
    TRANSACTION = "transaction"
    SHARD_MANAGER = "shard_manager"
    COUNTER = "counter"
    PUBSUB = "pubsub"
    INDEXING = "indexing"
    AUDIT = "audit"
    STORAGE = "storage"
    TRAIN = "train"
    TRAIN_SERVER = "train_server"
    FEATURE_FLAGS = "feature_flags"
    NATS_MANAGER = "nats_manager"


def get_utility(ident: Union[Utility, str]):
    return MAIN.get(ident)


def set_utility(ident: Union[Utility, str], util: Any):
    MAIN[ident] = util


def clean_utility(ident: Union[Utility, str]):
    if ident in MAIN:
        del MAIN[ident]


async def get_storage(
    gcs_scopes: Optional[List[str]] = None, service_name: Optional[str] = None
) -> Storage:
    if Utility.STORAGE in MAIN:
        return MAIN[Utility.STORAGE]

    if storage_settings.file_backend == FileBackendConfig.S3:
        from nucliadb_utils.storages.s3 import S3Storage

        s3util = S3Storage(
            aws_client_id=storage_settings.s3_client_id,
            aws_client_secret=storage_settings.s3_client_secret,
            endpoint_url=storage_settings.s3_endpoint,
            verify_ssl=storage_settings.s3_verify_ssl,
            deadletter_bucket=extended_storage_settings.s3_deadletter_bucket,
            indexing_bucket=extended_storage_settings.s3_indexing_bucket,
            use_ssl=storage_settings.s3_ssl,
            region_name=storage_settings.s3_region_name,
            max_pool_connections=storage_settings.s3_max_pool_connections,
            bucket=storage_settings.s3_bucket,
        )
        set_utility(Utility.STORAGE, s3util)
        await s3util.initialize()
        logger.info("Configuring S3 Storage")

    elif storage_settings.file_backend == FileBackendConfig.GCS:
        from nucliadb_utils.storages.gcs import GCSStorage

        gcsutil = GCSStorage(
            url=storage_settings.gcs_endpoint_url,
            account_credentials=storage_settings.gcs_base64_creds,
            bucket=storage_settings.gcs_bucket,
            location=storage_settings.gcs_location,
            project=storage_settings.gcs_project,
            deadletter_bucket=extended_storage_settings.gcs_deadletter_bucket,
            indexing_bucket=extended_storage_settings.gcs_indexing_bucket,
            executor=ThreadPoolExecutor(extended_storage_settings.gcs_threads),
            labels=storage_settings.gcs_bucket_labels,
            scopes=gcs_scopes,
        )
        set_utility(Utility.STORAGE, gcsutil)
        await gcsutil.initialize(service_name)
        logger.info("Configuring GCS Storage")

    elif storage_settings.file_backend == FileBackendConfig.PG:
        from nucliadb_utils.storages.pg import PostgresStorage

        pgutil = PostgresStorage(storage_settings.driver_pg_url)  # type: ignore
        set_utility(Utility.STORAGE, pgutil)
        await pgutil.initialize()
        logger.info("Configuring Postgres Storage")

    elif storage_settings.file_backend == FileBackendConfig.LOCAL:
        if storage_settings.local_files is None:
            raise ConfigurationError("LOCAL_FILES env var not configured")
        from nucliadb_utils.storages.local import LocalStorage

        localutil = LocalStorage(
            local_testing_files=storage_settings.local_files,
        )
        set_utility(Utility.STORAGE, localutil)
        await localutil.initialize()
        logger.info("Configuring Local Storage")
    else:
        raise ConfigurationError(
            "Invalid storage settings, please configure FILE_BACKEND"
        )

    return MAIN[Utility.STORAGE]


def get_local_storage() -> LocalStorage:
    if "local_storage" not in MAIN:
        from nucliadb_utils.storages.local import LocalStorage

        MAIN["local_storage"] = LocalStorage(
            local_testing_files=extended_storage_settings.local_testing_files
        )
        logger.info("Configuring Local Storage")
    return MAIN.get("local_storage", None)


async def get_nuclia_storage() -> NucliaStorage:
    if "nuclia_storage" not in MAIN:
        from nucliadb_utils.storages.nuclia import NucliaStorage

        MAIN["nuclia_storage"] = NucliaStorage(
            nuclia_public_url=nuclia_settings.nuclia_public_url,
            nuclia_zone=nuclia_settings.nuclia_zone,
            service_account=nuclia_settings.nuclia_service_account,
        )
        logger.info("Configuring Nuclia Storage")
        await MAIN["nuclia_storage"].initialize()
    return MAIN.get("nuclia_storage", None)


async def get_pubsub() -> Optional[PubSubDriver]:
    driver: Optional[PubSubDriver] = get_utility(Utility.PUBSUB)
    if driver is None:
        if cache_settings.cache_pubsub_nats_url:
            logger.info("Configuring nats pubsub")
            driver = NatsPubsub(
                hosts=cache_settings.cache_pubsub_nats_url,
                user_credentials_file=cache_settings.cache_pubsub_nats_auth,
            )
            set_utility(Utility.PUBSUB, driver)
        else:
            return None
    if not driver.initialized:
        await driver.initialize()
    return driver


def get_ingest() -> WriterStub:
    return get_utility(Utility.INGEST)  # type: ignore


def start_partitioning_utility() -> PartitionUtility:
    util = PartitionUtility(
        partitions=nuclia_settings.nuclia_partitions,
        seed=nuclia_settings.nuclia_hash_seed,
    )
    set_utility(Utility.PARTITION, util)
    return util


def stop_partitioning_utility():
    clean_utility(Utility.PARTITION)


def get_partitioning() -> PartitionUtility:
    return get_utility(Utility.PARTITION)  # type: ignore


def clear_global_cache():
    MAIN.clear()


async def finalize_utilities():
    to_delete = []
    for key, util in MAIN.items():
        if hasattr(util, "finalize") and asyncio.iscoroutinefunction(util.finalize):
            await util.finalize()
        elif hasattr(util, "finalize"):
            util.finalize()
        to_delete.append(key)
    for util in to_delete:
        clean_utility(util)


async def start_transaction_utility(
    service_name: Optional[str] = None,
) -> TransactionUtility:
    from nucliadb_utils.transaction import LocalTransactionUtility, TransactionUtility

    current = get_transaction_utility()
    if current is not None:
        logger.debug("Warning, transaction utility was already set, ignoring")
        return current

    if transaction_settings.transaction_local:
        transaction_utility: Union[
            LocalTransactionUtility, TransactionUtility
        ] = LocalTransactionUtility()
    elif transaction_settings.transaction_jetstream_servers is not None:
        transaction_utility = TransactionUtility(
            nats_creds=transaction_settings.transaction_jetstream_auth,
            nats_servers=transaction_settings.transaction_jetstream_servers,
        )
        await transaction_utility.initialize(service_name)
    set_utility(Utility.TRANSACTION, transaction_utility)
    return transaction_utility  # type: ignore


def get_transaction_utility() -> TransactionUtility:
    return get_utility(Utility.TRANSACTION)


async def stop_transaction_utility() -> None:
    transaction_utility = get_transaction_utility()
    if transaction_utility:
        await transaction_utility.finalize()
        clean_utility(Utility.TRANSACTION)


async def start_indexing_utility(service_name: Optional[str] = None) -> IndexingUtility:
    indexing_utility = IndexingUtility(
        nats_creds=indexing_settings.index_jetstream_auth,
        nats_servers=indexing_settings.index_jetstream_servers,
    )
    await indexing_utility.initialize(service_name)
    set_utility(Utility.INDEXING, indexing_utility)
    return indexing_utility


async def stop_indexing_utility():
    indexing_utility = get_indexing()
    if indexing_utility:
        await indexing_utility.finalize()
        clean_utility(Utility.INDEXING)


def get_indexing() -> IndexingUtility:
    return get_utility(Utility.INDEXING)


def get_audit() -> Optional[AuditStorage]:
    return get_utility(Utility.AUDIT)


async def start_audit_utility(service: str):
    audit_utility: Optional[AuditStorage] = get_utility(Utility.AUDIT)
    if audit_utility is not None:
        return

    if audit_settings.audit_driver == "basic":
        audit_utility = BasicAuditStorage()
        logger.info("Configuring basic audit log")
    elif audit_settings.audit_driver == "stream":
        audit_utility = StreamAuditStorage(
            nats_creds=audit_settings.audit_jetstream_auth,
            nats_servers=audit_settings.audit_jetstream_servers,
            nats_target=cast(str, audit_settings.audit_jetstream_target),
            partitions=audit_settings.audit_partitions,
            seed=audit_settings.audit_hash_seed,
            service=service,
        )
        logger.info(
            f"Configuring stream audit log {audit_settings.audit_jetstream_target}"
        )
    else:
        raise ConfigurationError("Invalid audit driver")
    await audit_utility.initialize()
    set_utility(Utility.AUDIT, audit_utility)


async def stop_audit_utility():
    audit_utility = get_audit()
    if audit_utility:
        await audit_utility.finalize()
        clean_utility(Utility.AUDIT)


async def start_nats_manager(
    service_name: str, nats_servers: list[str], nats_creds: Optional[str] = None
) -> NatsConnectionManager:
    nats_manager = NatsConnectionManager(
        service_name=service_name,
        nats_servers=nats_servers,
        nats_creds=nats_creds,
    )
    await nats_manager.initialize()
    set_utility(Utility.NATS_MANAGER, nats_manager)
    return nats_manager


def get_nats_manager() -> NatsConnectionManager:
    nats_manager = get_utility(Utility.NATS_MANAGER)
    if nats_manager is None:
        raise ConfigurationError("Nats manager not configured")
    return nats_manager


async def stop_nats_manager() -> None:
    try:
        nats_manager = get_nats_manager()
    except ConfigurationError:
        return
    await nats_manager.finalize()
    clean_utility(Utility.NATS_MANAGER)


def get_feature_flags() -> featureflagging.FlagService:
    val = get_utility(Utility.FEATURE_FLAGS)
    if val is None:
        val = featureflagging.FlagService()
        set_utility(Utility.FEATURE_FLAGS, val)
    return val


X_USER_HEADER = "X-NUCLIADB-USER"
X_ACCOUNT_HEADER = "X-NUCLIADB-ACCOUNT"
X_ACCOUNT_TYPE_HEADER = "X-NUCLIADB-ACCOUNT-TYPE"


def has_feature(
    name: str,
    default: bool = False,
    context: Optional[dict[str, str]] = None,
    headers: Optional[dict[str, str]] = None,
) -> bool:
    if context is None:
        context = {}
    if headers is not None:
        if X_USER_HEADER in headers:
            context["user_id_md5"] = hashlib.md5(
                headers[X_USER_HEADER].encode("utf-8")
            ).hexdigest()
        if X_ACCOUNT_HEADER in headers:
            context["account_id_md5"] = hashlib.md5(
                headers[X_ACCOUNT_HEADER].encode()
            ).hexdigest()
        if X_ACCOUNT_TYPE_HEADER in headers:
            context["account_type"] = headers[X_ACCOUNT_TYPE_HEADER]
    return get_feature_flags().enabled(name, default=default, context=context)
