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
from nucliadb_utils.utilities import Utility, clean_utility, get_utility, set_utility

try:
    from nidx_protos.nidx_pb2_grpc import NidxApiStub, NidxSearcherStub

    NIDX_INSTALLED = True
except ImportError:
    logger.info("nidx not installed")
    NIDX_INSTALLED = False


class NidxUtility:
    api_client = None
    searcher_client = None

    async def initialize(self): ...
    async def finalize(self): ...
    async def index(self, msg: IndexMessage) -> int: ...


class NidxBindingUtility(NidxUtility):
    """Implements Nidx utility using the binding"""

    def __init__(self):
        if ingest_settings.driver != DriverConfig.PG:
            raise ValueError("nidx_binding requires DRIVER=pg")

        if storage_settings.file_backend != FileBackendConfig.LOCAL:
            # This is a limitation just to simplify configuration, it can be removed if needed
            raise ValueError("nidx_binding requires FILE_BACKEND=local")

        if storage_settings.local_files is None or storage_settings.local_indexing_bucket is None:
            raise ValueError("nidx_binding requires LOCAL_FILES and LOCAL_INDEXING_BUCKET to be set")

        indexing_path = storage_settings.local_files + "/" + storage_settings.local_indexing_bucket
        nidx_storage_path = storage_settings.local_files + "/nidx"
        os.makedirs(indexing_path, exist_ok=True)
        os.makedirs(nidx_storage_path, exist_ok=True)

        self.config = {
            "METADATA__DATABASE_URL": ingest_settings.driver_pg_url,
            "INDEXER__OBJECT_STORE": "file",
            "INDEXER__FILE_PATH": indexing_path,
            "STORAGE__OBJECT_STORE": "file",
            "STORAGE__FILE_PATH": nidx_storage_path,
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
        raise "Not implemented yet"


class NidxServiceUtility(NidxUtility):
    """Implements Nidx utility connecting to the network service"""

    def __init__(self):
        if indexing_settings.index_nidx_subject is None:
            raise ValueError("nidx subject needed for nidx utility")

        if not settings.nidx_api_address or not settings.nidx_searcher_address:
            raise ValueError("NIDX_API and NIDX_SEARCHER are required")

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
    if not NIDX_INSTALLED:
        return None

    nidx = get_nidx()
    if nidx:
        logger.warn("nidx already initialized, will not reinitialize")
        return nidx

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


def get_nidx_api_client() -> Optional[NidxApiStub]:
    nidx = get_nidx()
    if nidx:
        return nidx.api_client
    else:
        return None


def get_nidx_searcher_client() -> Optional[NidxSearcherStub]:
    nidx = get_nidx()
    if nidx:
        return nidx.searcher_client
    else:
        return None
