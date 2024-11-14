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
from typing import Optional

from nidx_protos.nidx_pb2_grpc import NidxApiStub, NidxSearcherStub

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.settings import settings
from nucliadb.ingest.settings import DriverConfig
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb_protos.nodewriter_pb2 import (
    IndexMessage,
)
from nucliadb_utils import logger
from nucliadb_utils.grpc import get_traced_grpc_channel
from nucliadb_utils.nats import NatsConnectionManager
from nucliadb_utils.settings import FileBackendConfig, indexing_settings, storage_settings
from nucliadb_utils.storages.settings import settings as extended_storage_settings
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility

NIDX_ENABLED = bool(os.environ.get("NIDX_ENABLED"))


class NidxUtility:
    api_client = None
    searcher_client = None

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

    def __init__(self):
        if ingest_settings.driver != DriverConfig.PG:
            raise ValueError("nidx_binding requires DRIVER=pg")

        self.config = {
            "METADATA__DATABASE_URL": ingest_settings.driver_pg_url,
            **_storage_config("INDEXER", None),
            **_storage_config("STORAGE", "nidx"),
        }

    async def initialize(self):
        import nidx_binding  # type: ignore

        self.binding = nidx_binding.NidxBinding(self.config)
        self.api_client = NidxApiStub(
            get_traced_grpc_channel(f"localhost:{self.binding.api_port}", "nidx_api")
        )
        self.searcher_client = NidxSearcherStub(
            get_traced_grpc_channel(f"localhost:{self.binding.searcher_port}", "nidx_searcher")
        )

    async def finalize(self):
        del self.binding

    async def index(self, msg: IndexMessage) -> int:
        return self.binding.index(msg.SerializeToString())

    def wait_for_sync(self):
        self.binding.wait_for_sync()


class NidxServiceUtility(NidxUtility):
    """Implements Nidx utility connecting to the network service"""

    def __init__(self):
        if indexing_settings.index_nidx_subject is None:
            raise ValueError("INDEX_NIDX_SUBJECT needed for nidx utility")

        if not settings.nidx_api_address or not settings.nidx_searcher_address:
            raise ValueError("NIDX_API_ADDRESS and NIDX_SEARCHER_ADDRESS are required")

        self.nats_connection_manager = NatsConnectionManager(
            service_name="NidxIndexer",
            nats_servers=indexing_settings.index_jetstream_servers,
            nats_creds=indexing_settings.index_jetstream_auth,
        )
        self.subject = indexing_settings.index_nidx_subject

    async def initialize(self):
        await self.nats_connection_manager.initialize()
        self.api_client = NidxApiStub(get_traced_grpc_channel(settings.nidx_api_address, "nidx_api"))
        self.searcher_client = NidxSearcherStub(
            get_traced_grpc_channel(settings.nidx_searcher_address, "nidx_searcher")
        )

    async def finalize(self):
        await self.nats_connection_manager.finalize()

    async def index(self, writer: IndexMessage) -> int:
        res = await self.nats_connection_manager.js.publish(self.subject, writer.SerializeToString())
        logger.info(
            f" = Pushed message to nidx shard: {writer.shard}, txid: {writer.txid}  seqid: {res.seq}"  # noqa
        )
        return res.seq


async def start_nidx_utility() -> Optional[NidxUtility]:
    if not NIDX_ENABLED:
        return None

    nidx = get_nidx()
    if nidx:
        return nidx

    nidx_utility: NidxUtility
    if settings.standalone_mode:
        nidx_utility = NidxBindingUtility()
    else:
        nidx_utility = NidxServiceUtility()

    await nidx_utility.initialize()
    set_utility(Utility.NIDX, nidx_utility)
    return nidx_utility


async def stop_nidx_utility():
    nidx_utility = get_nidx()
    if nidx_utility:
        clean_utility(Utility.NIDX)
        await nidx_utility.finalize()


def get_nidx() -> Optional[NidxUtility]:
    return get_utility(Utility.NIDX)


def get_nidx_api_client() -> Optional["NidxApiStub"]:
    nidx = get_nidx()
    if nidx:
        return nidx.api_client
    else:
        return None


def get_nidx_searcher_client() -> Optional["NidxSearcherStub"]:
    nidx = get_nidx()
    if nidx:
        return nidx.searcher_client
    else:
        return None


# TODO: Remove the index node abstraction
class NodeNidxAdapter:
    def __init__(self, api_client, searcher_client):
        # API methods
        self.GetShard = api_client.GetShard
        self.NewShard = api_client.NewShard
        self.DeleteShard = api_client.DeleteShard
        self.ListShards = api_client.ListShards
        self.AddVectorSet = api_client.AddVectorSet
        self.RemoveVectorSet = api_client.RemoveVectorSet
        self.ListVectorSets = api_client.ListVectorSets
        self.GetMetadata = api_client.GetMetadata

        # Searcher methods
        self.Search = searcher_client.Search
        self.Suggest = searcher_client.Suggest
        self.Paragraphs = searcher_client.Paragraphs
        self.Documents = searcher_client.Documents


class FakeNode(AbstractIndexNode):
    def __init__(self, api_client, searcher_client):
        self.client = NodeNidxAdapter(api_client, searcher_client)

    @property
    def reader(self):
        return self.client

    @property
    def writer(self):
        return self.client

    def is_read_replica(_):
        return False

    @property
    def id(self):
        return "nidx"

    @property
    def address(self):
        return "nidx"

    @property
    def primary_id(self):
        return "nidx"


def get_nidx_fake_node() -> Optional[FakeNode]:
    nidx = get_nidx()
    if nidx:
        return FakeNode(nidx.api_client, nidx.searcher_client)
    else:
        return None
