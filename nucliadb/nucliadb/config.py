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

import os

from nucliadb.settings import Driver, Settings


def cleanup_config():
    from nucliadb.ingest import settings as ingest_settings
    from nucliadb.ingest.orm import NODE_CLUSTER
    from nucliadb.train import settings as train_settings
    from nucliadb.writer import settings as writer_settings
    from nucliadb_utils import settings as utils_settings
    from nucliadb_utils.cache import settings as cache_settings

    ingest_settings.settings.parse_obj(ingest_settings.Settings())
    train_settings.settings.parse_obj(train_settings.Settings())
    writer_settings.settings.parse_obj(writer_settings.Settings())
    cache_settings.settings.parse_obj(cache_settings.Settings())

    utils_settings.audit_settings.parse_obj(utils_settings.AuditSettings())
    utils_settings.indexing_settings.parse_obj(utils_settings.IndexingSettings())
    utils_settings.transaction_settings.parse_obj(utils_settings.TransactionSettings())
    utils_settings.nucliadb_settings.parse_obj(utils_settings.NucliaDBSettings())
    utils_settings.nuclia_settings.parse_obj(utils_settings.NucliaSettings())
    utils_settings.storage_settings.parse_obj(utils_settings.StorageSettings())

    NODE_CLUSTER.local_node = None


def config_nucliadb(nucliadb_args: Settings):
    from nucliadb.ingest.orm import NODE_CLUSTER
    from nucliadb.ingest.orm.local_node import LocalNode
    from nucliadb.ingest.settings import settings as ingest_settings
    from nucliadb.train.settings import settings as train_settings
    from nucliadb.writer.settings import settings as writer_settings
    from nucliadb_utils.cache.settings import settings as cache_settings
    from nucliadb_utils.settings import (
        audit_settings,
        http_settings,
        indexing_settings,
        nuclia_settings,
        nucliadb_settings,
        running_settings,
        storage_settings,
        transaction_settings,
    )

    ingest_settings.chitchat_enabled = False
    ingest_settings.nuclia_partitions = 1
    ingest_settings.total_replicas = 1
    ingest_settings.replica_number = 0
    ingest_settings.partitions = ["1"]
    running_settings.debug = True
    nuclia_settings.onprem = True
    http_settings.cors_origins = ["*"]
    nucliadb_settings.nucliadb_ingest = None
    transaction_settings.transaction_local = True
    audit_settings.audit_driver = "basic"
    indexing_settings.index_local = True
    cache_settings.cache_enabled = False
    writer_settings.dm_enabled = False

    running_settings.log_level = nucliadb_args.log.upper()
    running_settings.activity_log_level = nucliadb_args.log.upper()
    train_settings.grpc_port = nucliadb_args.train
    ingest_settings.grpc_port = nucliadb_args.grpc

    if nucliadb_args.driver == Driver.LOCAL:
        ingest_settings.driver = "local"
        ingest_settings.driver_local_url = nucliadb_args.maindb
        if not os.path.isdir(nucliadb_args.maindb):
            os.makedirs(nucliadb_args.maindb, exist_ok=True)
    elif nucliadb_args.driver == Driver.REDIS:
        ingest_settings.driver = "redis"
        ingest_settings.driver_redis_url = nucliadb_args.maindb

    storage_settings.file_backend = "local"

    if not os.path.isdir(nucliadb_args.blob):
        os.makedirs(nucliadb_args.blob, exist_ok=True)
    storage_settings.local_files = nucliadb_args.blob

    os.environ["DATA_PATH"] = nucliadb_args.node
    if not os.path.isdir(nucliadb_args.node):
        os.makedirs(nucliadb_args.node, exist_ok=True)

    if nucliadb_args.key is None:
        if os.environ.get("NUA_API_KEY"):
            nuclia_settings.nuclia_service_account = os.environ.get("NUA_API_KEY")
            nuclia_settings.disable_send_to_process = False
            ingest_settings.pull_time = 60

        else:
            ingest_settings.pull_time = 0
            nuclia_settings.disable_send_to_process = True
    else:
        nuclia_settings.nuclia_service_account = nucliadb_args.key

    if nucliadb_args.zone is not None:
        nuclia_settings.nuclia_zone = nucliadb_args.zone
    elif os.environ.get("NUA_ZONE"):
        nuclia_settings.nuclia_zone = os.environ.get("NUA_ZONE", "dev")

    local_node = LocalNode()
    NODE_CLUSTER.local_node = local_node
