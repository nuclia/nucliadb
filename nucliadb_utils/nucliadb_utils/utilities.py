from __future__ import annotations

import asyncio
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum
from typing import TYPE_CHECKING, Any, List, Optional

from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.audit.basic import BasicAuditStorage
from nucliadb_utils.audit.stream import StreamAuditStorage
from nucliadb_utils.cache.nats import NatsPubsub
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.cache.redis import RedisPubsub
from nucliadb_utils.cache.settings import settings as cache_settings
from nucliadb_utils.cache.utility import Cache
from nucliadb_utils.indexing import IndexingUtility
from nucliadb_utils.partition import PartitionUtility
from nucliadb_utils.settings import (
    audit_settings,
    nuclia_settings,
    running_settings,
    storage_settings,
)
from nucliadb_utils.storages.settings import settings as extended_storage_settings
from nucliadb_utils.store import MAIN
from nucliadb_utils.transaction import TransactionUtility

if TYPE_CHECKING:
    from nucliadb_utils.storages.local import LocalStorage
    from nucliadb_utils.storages.nuclia import NucliaStorage
    from nucliadb_utils.storages.storage import Storage


class Utility(str, Enum):
    INGEST = "ingest"
    CHANNEL = "channel"
    PARTITION = "partition"
    PREDICT = "predict"
    PROCESSING = "processing"
    TRANSACTION = "transaction"
    CACHE = "cache"
    NODES = "nodes"
    COUNTER = "counter"
    SWIM = "swim"
    PUBSUB = "pubsub"
    INDEXING = "indexing"
    AUDIT = "audit"
    STORAGE = "storage"


def get_utility(ident: Utility):
    return MAIN.get(ident)


def set_utility(ident: Utility, util: Any):
    MAIN[ident] = util


def clean_utility(ident: Utility):
    if ident in MAIN:
        del MAIN[ident]


async def get_storage(gcs_scopes: Optional[List[str]] = None) -> Storage:

    if storage_settings.file_backend == "s3" and Utility.STORAGE not in MAIN:
        from nucliadb_utils.storages.s3 import S3Storage

        if extended_storage_settings.s3_deadletter_bucket is None:
            raise RuntimeError("Missing s3_deadletter_bucket setting")

        if extended_storage_settings.s3_indexing_bucket is None:
            raise RuntimeError("Missing s3_deadletter_bucket setting")

        util = S3Storage(
            aws_client_id=storage_settings.s3_client_id,
            aws_client_secret=storage_settings.s3_client_secret,
            endpoint_url=storage_settings.s3_endpoint,
            verify_ssl=storage_settings.s3_verify_ssl,
            deadletter_bucket=extended_storage_settings.s3_deadletter_bucket.format(
                zone=nuclia_settings.nuclia_zone,
                env=running_settings.running_environment,
            ),
            indexing_bucket=extended_storage_settings.s3_indexing_bucket.format(
                zone=nuclia_settings.nuclia_zone,
                env=running_settings.running_environment,
            ),
            use_ssl=storage_settings.s3_ssl,
            region_name=storage_settings.s3_region_name,
            max_pool_connections=storage_settings.s3_max_pool_connections,
            bucket=storage_settings.s3_bucket,
        )

        set_utility(Utility.STORAGE, util)
        await util.initialize()

    elif storage_settings.file_backend == "gcs" and Utility.STORAGE not in MAIN:
        from nucliadb_utils.storages.gcs import GCSStorage

        if extended_storage_settings.gcs_deadletter_bucket is None:
            raise RuntimeError("Missing gcs_deadletter_bucket setting")

        if extended_storage_settings.gcs_indexing_bucket is None:
            raise RuntimeError("Missing gcs_deadletter_bucket setting")

        gcs_util = GCSStorage(
            url=storage_settings.gcs_endpoint_url,
            account_credentials=storage_settings.gcs_base64_creds,
            bucket=storage_settings.gcs_bucket,
            location=storage_settings.gcs_location,
            project=storage_settings.gcs_project,
            deadletter_bucket=extended_storage_settings.gcs_deadletter_bucket.format(
                zone=nuclia_settings.nuclia_zone,
                env=running_settings.running_environment,
            ),
            indexing_bucket=extended_storage_settings.gcs_indexing_bucket.format(
                zone=nuclia_settings.nuclia_zone,
                env=running_settings.running_environment,
            ),
            executor=ThreadPoolExecutor(extended_storage_settings.gcs_threads),
            labels=storage_settings.gcs_bucket_labels,
            scopes=gcs_scopes,
        )
        set_utility(Utility.STORAGE, gcs_util)
        await gcs_util.initialize()

    if MAIN.get(Utility.STORAGE) is None:
        raise AttributeError()

    return MAIN[Utility.STORAGE]


def get_local_storage() -> LocalStorage:
    if "local_storage" not in MAIN:
        from nucliadb_utils.storages.local import LocalStorage

        MAIN["local_storage"] = LocalStorage(
            local_testing_files=extended_storage_settings.local_testing_files
        )
    return MAIN.get("local_storage", None)


def get_nuclia_storage() -> NucliaStorage:
    if "nuclia_storage" not in MAIN:
        from nucliadb_utils.storages.nuclia import NucliaStorage

        MAIN["nuclia_storage"] = NucliaStorage(
            service_account=nuclia_settings.nuclia_service_account
        )
    return MAIN.get("nuclia_storage", None)


async def get_cache() -> Optional[Cache]:
    util = get_utility(Utility.CACHE)
    if util is None:
        driver = Cache()
        set_utility(Utility.CACHE, driver)
    cache = get_utility(Utility.CACHE)
    if cache and not cache.initialized:
        await cache.initialize()
    return cache


async def get_pubsub() -> PubSubDriver:
    driver: Optional[PubSubDriver] = get_utility(Utility.PUBSUB)
    if cache_settings.cache_pubsub_driver == "redis" and driver is None:
        driver = RedisPubsub(cache_settings.cache_pubsub_redis_url)
        set_utility(Utility.PUBSUB, driver)
    elif cache_settings.cache_pubsub_driver == "nats" and driver is None:
        driver = NatsPubsub(
            hosts=cache_settings.cache_pubsub_nats_url,
            user_credentials_file=cache_settings.cache_pubsub_nats_auth,
        )
        set_utility(Utility.PUBSUB, driver)
    elif driver is None:
        raise NotImplementedError("Invalid driver")

    if driver and not driver.initialized:
        await driver.initialize()
    return driver


def get_ingest() -> WriterStub:
    return get_utility(Utility.INGEST)  # type: ignore


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


def get_transaction() -> TransactionUtility:
    return get_utility(Utility.TRANSACTION)


def get_indexing() -> IndexingUtility:
    return get_utility(Utility.INDEXING)


def get_audit() -> AuditStorage:
    return get_utility(Utility.AUDIT)


async def start_audit_utility():
    if audit_settings.audit_driver == "basic":
        audit_utility = BasicAuditStorage()
    elif audit_settings.audit_driver == "stream":
        audit_utility = StreamAuditStorage(
            nats_creds=audit_settings.audit_jetstream_auth,
            nats_servers=audit_settings.audit_jetstream_servers,
            nats_target=audit_settings.audit_jetstream_target,
            partitions=audit_settings.audit_partitions,
            seed=audit_settings.audit_hash_seed,
        )
    await audit_utility.initialize()
    set_utility(Utility.AUDIT, audit_utility)


async def stop_audit_utility():
    audit_utility = get_audit()
    if audit_utility:
        await audit_utility.finalize()
        clean_utility(Utility.AUDIT)
