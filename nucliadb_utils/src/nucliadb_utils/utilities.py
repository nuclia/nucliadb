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
from nucliadb_telemetry.metrics import Counter
from nucliadb_utils import featureflagging
from nucliadb_utils.aiopynecone.client import PineconeSession
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.audit.basic import BasicAuditStorage
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.cache.nats import NatsPubsub
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.cache.settings import settings as cache_settings
from nucliadb_utils.encryption import EndecryptorUtility
from nucliadb_utils.encryption.settings import settings as encryption_settings
from nucliadb_utils.exceptions import ConfigurationError
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.partition import PartitionUtility
from nucliadb_utils.settings import (
    FileBackendConfig,
    audit_settings,
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

pull_subscriber_utilization = Counter(
    "nucliadb_pull_subscriber_utilization_seconds", labels={"status": ""}
)


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
    AUDIT = "audit"
    STORAGE = "storage"
    TRAIN = "train"
    TRAIN_SERVER = "train_server"
    FEATURE_FLAGS = "feature_flags"
    NATS_MANAGER = "nats_manager"
    LOCAL_STORAGE = "local_storage"
    NUCLIA_STORAGE = "nuclia_storage"
    MAINDB_DRIVER = "driver"
    USAGE = "usage"
    ENDECRYPTOR = "endecryptor"
    PINECONE_SESSION = "pinecone_session"
    NIDX = "nidx"


def get_utility(ident: Union[Utility, str]):
    return MAIN.get(ident)


def set_utility(ident: Union[Utility, str], util: Any):
    if ident in MAIN:
        logger.warning(f"Overwriting previously set utility {ident}: {MAIN[ident]} with {util}")
    MAIN[ident] = util


def clean_utility(ident: Union[Utility, str]):
    MAIN.pop(ident, None)


async def get_storage(
    gcs_scopes: Optional[List[str]] = None, service_name: Optional[str] = None
) -> Storage:
    if Utility.STORAGE in MAIN:
        return MAIN[Utility.STORAGE]

    storage = await _create_storage(gcs_scopes=gcs_scopes)
    set_utility(Utility.STORAGE, storage)

    return MAIN[Utility.STORAGE]


async def _create_storage(gcs_scopes: Optional[List[str]] = None) -> Storage:
    if storage_settings.file_backend == FileBackendConfig.AZURE:
        from nucliadb_utils.storages.azure import AzureStorage

        if storage_settings.azure_account_url is None:
            raise ConfigurationError("AZURE_ACCOUNT_URL env variable not configured")

        azureutil = AzureStorage(
            account_url=storage_settings.azure_account_url,
            connection_string=storage_settings.azure_connection_string,
        )

        logger.info("Configuring Azure Storage")
        await azureutil.initialize()
        return azureutil

    elif storage_settings.file_backend == FileBackendConfig.S3:
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
            bucket_tags=storage_settings.s3_bucket_tags,
            kms_key_id=storage_settings.s3_kms_key_id,
        )
        logger.info("Configuring S3 Storage")
        await s3util.initialize()
        return s3util

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
        logger.info("Configuring GCS Storage")
        await gcsutil.initialize()
        return gcsutil

    elif storage_settings.file_backend == FileBackendConfig.LOCAL:
        if storage_settings.local_files is None:
            raise ConfigurationError("LOCAL_FILES env var not configured")
        from nucliadb_utils.storages.local import LocalStorage

        localutil = LocalStorage(
            local_testing_files=storage_settings.local_files,
            indexing_bucket=storage_settings.local_indexing_bucket,
        )
        logger.info("Configuring Local Storage")
        await localutil.initialize()
        return localutil
    else:
        raise ConfigurationError("Invalid storage settings, please configure FILE_BACKEND")


async def teardown_storage() -> None:
    storage: Optional[Storage] = get_utility(Utility.STORAGE)
    if storage is None:
        return
    await storage.finalize()
    clean_utility(Utility.STORAGE)


def get_local_storage() -> LocalStorage:
    if Utility.LOCAL_STORAGE not in MAIN:
        from nucliadb_utils.storages.local import LocalStorage

        MAIN[Utility.LOCAL_STORAGE] = LocalStorage(
            local_testing_files=extended_storage_settings.local_testing_files
        )
        logger.info("Configuring Local Storage")
    return MAIN.get(Utility.LOCAL_STORAGE, None)


async def get_nuclia_storage() -> NucliaStorage:
    if Utility.NUCLIA_STORAGE not in MAIN:
        from nucliadb_utils.storages.nuclia import NucliaStorage

        MAIN[Utility.NUCLIA_STORAGE] = NucliaStorage(
            nuclia_public_url=nuclia_settings.nuclia_public_url,
            nuclia_zone=nuclia_settings.nuclia_zone,
            service_account=nuclia_settings.nuclia_service_account,
        )
        logger.info("Configuring Nuclia Storage")
        await MAIN[Utility.NUCLIA_STORAGE].initialize()
    return MAIN.get(Utility.NUCLIA_STORAGE, None)


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
    util = get_utility(Utility.PARTITION)
    if util is not None:
        return util

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
        transaction_utility: Union[LocalTransactionUtility, TransactionUtility] = (
            LocalTransactionUtility()
        )
    elif transaction_settings.transaction_jetstream_servers is not None:
        transaction_utility = TransactionUtility(
            nats_creds=transaction_settings.transaction_jetstream_auth,
            nats_servers=transaction_settings.transaction_jetstream_servers,
            commit_timeout=transaction_settings.transaction_commit_timeout,
        )
        await transaction_utility.initialize(service_name)
    set_utility(Utility.TRANSACTION, transaction_utility)
    return transaction_utility


def get_transaction_utility() -> TransactionUtility:
    return get_utility(Utility.TRANSACTION)


async def stop_transaction_utility() -> None:
    transaction_utility = get_transaction_utility()
    if transaction_utility:
        await transaction_utility.finalize()
        clean_utility(Utility.TRANSACTION)


def get_audit() -> Optional[AuditStorage]:
    return get_utility(Utility.AUDIT)


def register_audit_utility(service: str) -> AuditStorage:
    if audit_settings.audit_driver == "basic":
        b_audit_utility: AuditStorage = BasicAuditStorage()
        set_utility(Utility.AUDIT, b_audit_utility)
        logger.info("Configuring basic audit log")
        return b_audit_utility
    elif audit_settings.audit_driver == "stream":
        s_audit_utility: AuditStorage = StreamAuditStorage(
            nats_creds=audit_settings.audit_jetstream_auth,
            nats_servers=audit_settings.audit_jetstream_servers,
            nats_target=cast(str, audit_settings.audit_jetstream_target),
            partitions=audit_settings.audit_partitions,
            seed=audit_settings.audit_hash_seed,
            service=service,
        )
        set_utility(Utility.AUDIT, s_audit_utility)
        logger.info(f"Configuring stream audit log {audit_settings.audit_jetstream_target}")
        return s_audit_utility
    else:
        raise ConfigurationError("Invalid audit driver")


async def start_audit_utility(service: str):
    audit_utility: Optional[AuditStorage] = get_utility(Utility.AUDIT)
    if audit_utility is not None and audit_utility.initialized is True:
        return

    if audit_utility is None:
        audit_utility = register_audit_utility(service)
    if audit_utility.initialized is False:
        await audit_utility.initialize()


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
        pull_utilization_metrics=pull_subscriber_utilization,
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
            context["user_id_md5"] = hashlib.md5(headers[X_USER_HEADER].encode("utf-8")).hexdigest()
        if X_ACCOUNT_HEADER in headers:
            context["account_id_md5"] = hashlib.md5(headers[X_ACCOUNT_HEADER].encode()).hexdigest()
        if X_ACCOUNT_TYPE_HEADER in headers:
            context["account_type"] = headers[X_ACCOUNT_TYPE_HEADER]
    return get_feature_flags().enabled(name, default=default, context=context)


def get_endecryptor() -> EndecryptorUtility:
    util = get_utility(Utility.ENDECRYPTOR)
    if util is not None:
        return util
    if encryption_settings.encryption_secret_key is None:
        raise ConfigurationError("Encryption secret key not configured")
    try:
        util = EndecryptorUtility.from_b64_encoded_secret_key(encryption_settings.encryption_secret_key)
    except ValueError as ex:
        raise ConfigurationError(
            "Invalid encryption key. Must be a base64 encoded 32-byte string"
        ) from ex
    set_utility(Utility.ENDECRYPTOR, util)
    return util


def get_pinecone() -> PineconeSession:
    util = get_utility(Utility.PINECONE_SESSION)
    if util is not None:
        return util
    util = PineconeSession()
    set_utility(Utility.PINECONE_SESSION, util)
    return util


def clean_pinecone():
    clean_utility(Utility.PINECONE_SESSION)
