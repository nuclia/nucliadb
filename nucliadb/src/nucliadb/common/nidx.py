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

import os
from typing import Optional, Union

from nidx_protos.nidx_pb2_grpc import NidxApiStub, NidxIndexerStub, NidxSearcherStub
from nidx_protos.nodewriter_pb2 import (
    IndexMessage,
)

from nucliadb.common.cluster.settings import settings
from nucliadb.ingest.settings import DriverConfig
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb_utils import logger
from nucliadb_utils.grpc import get_traced_grpc_channel
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import FileBackendConfig, indexing_settings, storage_settings
from nucliadb_utils.storages.settings import settings as extended_storage_settings
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility


class NidxUtility:
    api_client: NidxApiStub
    searcher_client: NidxSearcherStub

    async def initialize(self):
        raise NotImplementedError()

    async def finalize(self):
        raise NotImplementedError()

    async def index(self, msg: IndexMessage) -> int:
        raise NotImplementedError()

    def wait_for_sync(self):
        pass


def _storage_config(prefix: str, bucket: Optional[str]) -> dict[str, str]:
    config = {}
    if storage_settings.file_backend == FileBackendConfig.LOCAL:
        local_bucket = bucket or storage_settings.local_indexing_bucket
        file_path = f"{storage_settings.local_files}/{local_bucket}"
        os.makedirs(file_path, exist_ok=True)

        config[f"{prefix}__OBJECT_STORE"] = "file"
        config[f"{prefix}__FILE_PATH"] = file_path
    elif storage_settings.file_backend == FileBackendConfig.GCS:
        gcs_bucket = bucket or extended_storage_settings.gcs_indexing_bucket
        config[f"{prefix}__OBJECT_STORE"] = "gcs"
        if gcs_bucket:
            config[f"{prefix}__BUCKET"] = gcs_bucket
        if storage_settings.gcs_base64_creds:
            config[f"{prefix}__BASE64_CREDS"] = storage_settings.gcs_base64_creds
        if storage_settings.gcs_endpoint_url:
            config[f"{prefix}__ENDPOINT"] = storage_settings.gcs_endpoint_url
    elif storage_settings.file_backend == FileBackendConfig.S3:
        s3_bucket = bucket or extended_storage_settings.s3_indexing_bucket
        config[f"{prefix}__OBJECT_STORE"] = "s3"
        if s3_bucket:
            config[f"{prefix}__BUCKET"] = s3_bucket
        config[f"{prefix}__CLIENT_ID"] = storage_settings.s3_client_id or ""
        config[f"{prefix}__CLIENT_SECRET"] = storage_settings.s3_client_secret or ""
        config[f"{prefix}__REGION_NAME"] = storage_settings.s3_region_name or ""
        if storage_settings.s3_endpoint:
            config[f"{prefix}__ENDPOINT"] = storage_settings.s3_endpoint

    return config


class NidxBindingUtility(NidxUtility):
    """Implements Nidx utility using the binding"""

    def __init__(self, service_name: str):
        if ingest_settings.driver != DriverConfig.PG:
            raise ValueError("nidx_binding requires DRIVER=pg")

        self.service_name = service_name
        self.config = {
            "METADATA__DATABASE_URL": ingest_settings.driver_pg_url,
            "SEARCHER__METADATA_REFRESH_INTERVAL": str(
                indexing_settings.index_searcher_refresh_interval
            ),
            **_storage_config("INDEXER", None),
            **_storage_config("STORAGE", "nidx"),
        }

    async def initialize(self):
        import nidx_binding  # type: ignore

        self.binding = nidx_binding.NidxBinding(self.config)
        self.api_client = NidxApiStub(
            get_traced_grpc_channel(f"localhost:{self.binding.api_port}", self.service_name)
        )
        self.searcher_client = NidxSearcherStub(
            get_traced_grpc_channel(f"localhost:{self.binding.searcher_port}", self.service_name)
        )

    async def finalize(self):
        del self.binding

    async def index(self, msg: IndexMessage) -> int:
        return self.binding.index(msg.SerializeToString())

    def wait_for_sync(self):
        self.binding.wait_for_sync()


class NidxNatsIndexer:
    def __init__(self, service_name: str):
        self.nats_connection_manager = NatsConnectionManager(
            service_name=service_name,
            nats_servers=indexing_settings.index_jetstream_servers,
            nats_creds=indexing_settings.index_jetstream_auth,
        )
        assert indexing_settings.index_nidx_subject is not None
        self.subject = indexing_settings.index_nidx_subject

    async def initialize(self):
        await self.nats_connection_manager.initialize()

    async def finalize(self):
        await self.nats_connection_manager.finalize()

    async def index(self, writer: IndexMessage) -> int:
        res = await self.nats_connection_manager.js.publish(self.subject, writer.SerializeToString())
        logger.info(
            f" = Pushed message to nidx shard: {writer.shard}, txid: {writer.txid}  seqid: {res.seq}"  # noqa
        )
        return res.seq


class NidxGrpcIndexer:
    def __init__(self, address: str, service_name: str):
        self.address = address
        self.service_name = service_name

    async def initialize(self):
        self.client = NidxIndexerStub(get_traced_grpc_channel(self.address, self.service_name))

    async def finalize(self):
        pass

    async def index(self, writer: IndexMessage) -> int:
        await self.client.Index(writer)
        return 0


class NidxServiceUtility(NidxUtility):
    """Implements Nidx utility connecting to the network service"""

    indexer: Union[NidxNatsIndexer, NidxGrpcIndexer]

    def __init__(self, service_name: str):
        if not settings.nidx_api_address or not settings.nidx_searcher_address:
            raise ValueError("NIDX_API_ADDRESS and NIDX_SEARCHER_ADDRESS are required")

        if indexing_settings.index_nidx_subject:
            self.indexer = NidxNatsIndexer(service_name)
        elif settings.nidx_indexer_address is not None:
            self.indexer = NidxGrpcIndexer(settings.nidx_indexer_address, service_name)
        else:
            raise ValueError("NIDX_INDEXER_ADDRESS or INDEX_NIDX_SUBJECT are required")

    async def initialize(self):
        await self.indexer.initialize()
        self.api_client = NidxApiStub(get_traced_grpc_channel(settings.nidx_api_address, "nidx_api"))
        self.searcher_client = NidxSearcherStub(
            get_traced_grpc_channel(settings.nidx_searcher_address, "nidx_searcher")
        )

    async def finalize(self):
        await self.indexer.finalize()

    async def index(self, writer: IndexMessage) -> int:
        return await self.indexer.index(writer)


async def start_nidx_utility(service_name: str = "nucliadb.nidx") -> Optional[NidxUtility]:
    nidx = get_utility(Utility.NIDX)
    if nidx:
        return nidx

    nidx_utility: NidxUtility
    if settings.standalone_mode:
        if (
            settings.nidx_api_address is not None
            and settings.nidx_searcher_address is not None
            and settings.nidx_indexer_address is not None
        ):
            # Standalone with nidx service (via grpc). This is used in clustered standalone mode
            nidx_utility = NidxServiceUtility(service_name)
        else:
            # Normal standalone mode with binding
            nidx_utility = NidxBindingUtility(service_name)
    else:
        # Component deploy with nidx service via grpc & nats (cloud)
        nidx_utility = NidxServiceUtility(service_name)

    await nidx_utility.initialize()
    set_utility(Utility.NIDX, nidx_utility)
    return nidx_utility


async def stop_nidx_utility():
    nidx_utility = get_utility(Utility.NIDX)
    if nidx_utility:
        clean_utility(Utility.NIDX)
        await nidx_utility.finalize()


def get_nidx() -> NidxUtility:
    nidx = get_utility(Utility.NIDX)
    if nidx is None:
        raise Exception("nidx not initialized")
    return nidx


def get_nidx_api_client() -> "NidxApiStub":
    nidx = get_nidx()
    if nidx.api_client:
        return nidx.api_client
    else:
        raise Exception("nidx not initialized")


def get_nidx_searcher_client() -> "NidxSearcherStub":
    nidx = get_nidx()
    if nidx.searcher_client:
        return nidx.searcher_client
    else:
        raise Exception("nidx not initialized")
