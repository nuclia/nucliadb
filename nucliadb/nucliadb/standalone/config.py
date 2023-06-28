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


import logging
import os

from nucliadb.standalone.settings import Settings

logger = logging.getLogger(__name__)


def config_standalone_driver(nucliadb_args: Settings):
    from nucliadb.ingest.settings import DriverConfig, DriverSettings
    from nucliadb.ingest.settings import settings as ingest_settings
    from nucliadb_utils.settings import (
        FileBackendConfig,
        StorageSettings,
        storage_settings,
    )

    # update global settings with arg values
    for fieldname in DriverSettings.__fields__.keys():
        setattr(ingest_settings, fieldname, getattr(nucliadb_args, fieldname))
    for fieldname in StorageSettings.__fields__.keys():
        setattr(storage_settings, fieldname, getattr(nucliadb_args, fieldname))

    if ingest_settings.driver == DriverConfig.NOT_SET:
        # no driver specified, for standalone, we force defaulting to local here
        ingest_settings.driver = DriverConfig.LOCAL

    if (
        ingest_settings.driver == DriverConfig.LOCAL
        and ingest_settings.driver_local_url is None
    ):
        # also provide default path for local driver when none provided
        ingest_settings.driver_local_url = "./data/main"

    if storage_settings.file_backend == FileBackendConfig.NOT_SET:
        # no driver specified, for standalone, we try to automate some settings here
        storage_settings.file_backend = FileBackendConfig.LOCAL

    if (
        storage_settings.file_backend == FileBackendConfig.LOCAL
        and storage_settings.local_files is None
    ):
        storage_settings.local_files = "./data/blob"

    if ingest_settings.driver_local_url is not None and not os.path.isdir(
        ingest_settings.driver_local_url
    ):
        os.makedirs(ingest_settings.driver_local_url, exist_ok=True)

    # need to force inject this to env var
    if "DATA_PATH" not in os.environ:
        os.environ["DATA_PATH"] = nucliadb_args.data_path


def config_nucliadb(nucliadb_args: Settings):
    """
    Standalone nucliadb configuration forces us to
    use some specific settings.
    """

    from nucliadb.common.cluster import manager
    from nucliadb.common.cluster.settings import settings as cluster_settings
    from nucliadb.ingest.settings import settings as ingest_settings
    from nucliadb.train.settings import settings as train_settings
    from nucliadb.writer.settings import settings as writer_settings
    from nucliadb_utils.cache.settings import settings as cache_settings
    from nucliadb_utils.settings import (
        audit_settings,
        http_settings,
        nuclia_settings,
        nucliadb_settings,
        transaction_settings,
    )

    cluster_settings.standalone_mode = True
    cluster_settings.node_replicas = 1
    cluster_settings.data_path = nucliadb_args.data_path
    ingest_settings.nuclia_partitions = 1
    ingest_settings.total_replicas = 1
    ingest_settings.replica_number = 0
    ingest_settings.partitions = ["1"]
    nuclia_settings.onprem = True
    http_settings.cors_origins = ["*"]
    nucliadb_settings.nucliadb_ingest = None
    transaction_settings.transaction_local = True
    audit_settings.audit_driver = "basic"
    cache_settings.cache_enabled = False
    writer_settings.dm_enabled = False

    train_settings.grpc_port = nucliadb_args.train_grpc_port
    ingest_settings.grpc_port = nucliadb_args.ingest_grpc_port

    config_standalone_driver(nucliadb_args)

    if nucliadb_args.nua_api_key:
        nuclia_settings.nuclia_service_account = nucliadb_args.nua_api_key
    else:
        ingest_settings.disable_pull_worker = True
        nuclia_settings.dummy_processing = True
        nuclia_settings.dummy_predict = True

    if nucliadb_args.zone is not None:
        nuclia_settings.nuclia_zone = nucliadb_args.zone
    elif os.environ.get("NUA_ZONE"):
        nuclia_settings.nuclia_zone = os.environ.get("NUA_ZONE", "dev")

    manager.setup_standalone_cluster()
