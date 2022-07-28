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

import asyncio
import logging
import argparse
from typing import Optional
from nucliadb_ingest.orm import NODE_CLUSTER
from nucliadb_ingest.orm.local_node import LocalNode
from nucliadb_ingest.purge import main

import uvicorn
import os

logger = logging.getLogger("nucliadb")


# this is default (site-packages\uvicorn\main.py)
log_config = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(levelprefix)s %(message)s",
            "use_colors": None,
        },
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "fmt": '%(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
        },
        "access": {
            "formatter": "access",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {
        "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
        "uvicorn.error": {"level": "INFO", "handlers": ["default"], "propagate": False},
        "uvicorn.access": {"handlers": ["access"], "level": "INFO", "propagate": False},
        "nucliadb": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
        "nucliadb_one": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
        "nucliadb_ingest": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
        "nucliadb_search": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
        "nucliadb_writer": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
        "nucliadb_reader": {
            "level": "INFO",
            "handlers": ["default"],
            "propagate": True,
        },
    },
}


class Settings:
    maindb: str
    blob: str
    key: Optional[str] = None
    node: str
    host: Optional[str] = None
    zone: Optional[str] = None
    http: int
    grpc: int
    train: int
    log: str


def arg_parse():

    parser = argparse.ArgumentParser(description="Process some integers.")

    parser.add_argument(
        "-p", "--maindb", dest="maindb", help="MainDB data folder", required=True
    )

    parser.add_argument(
        "-b", "--blobstorage", dest="blob", help="Blob data folder", required=True
    )

    parser.add_argument("-k", "--key", dest="key", help="Understanding API Key")

    parser.add_argument(
        "-n", "--node", dest="node", help="Node data folder", required=True
    )

    parser.add_argument("-u", "--host", dest="host", help="Overwride host API")

    parser.add_argument("-z", "--zone", dest="zone", help="Understanding API Zone")

    parser.add_argument(
        "-g", "--grpc", dest="grpc", default=8030, help="NucliaDB GRPC Port", type=int
    )

    parser.add_argument(
        "-t",
        "--train",
        dest="train",
        default=8060,
        help="NucliaDB Train Port",
        type=int,
    )

    parser.add_argument(
        "-s", "--http", dest="http", default=8080, help="NucliaDB HTTP Port", type=int
    )

    parser.add_argument("--log", dest="log", default="INFO", help="LOG LEVEL")

    args = parser.parse_args()
    return args


def run():

    nucliadb_args = arg_parse()
    config_nucliadb(nucliadb_args)
    run_nucliadb(nucliadb_args)


def config_nucliadb(nucliadb_args: Settings):
    from nucliadb_ingest.settings import settings as ingest_settings
    from nucliadb_train.settings import settings as train_settings
    from nucliadb_search.settings import settings as search_settings
    from nucliadb_writer.settings import settings as writer_settings
    from nucliadb_utils.settings import (
        running_settings,
        http_settings,
        storage_settings,
        nuclia_settings,
        nucliadb_settings,
        transaction_settings,
        audit_settings,
        indexing_settings,
    )
    from nucliadb_utils.cache.settings import settings as cache_settings

    running_settings.log_level = nucliadb_args.log.upper()
    train_settings.grpc_port = nucliadb_args.train
    ingest_settings.driver = "local"
    ingest_settings.driver_local_url = nucliadb_args.maindb
    ingest_settings.chitchat_enabled = False
    running_settings.debug = True
    http_settings.cors_origins = ["*"]
    storage_settings.file_backend = "local"
    storage_settings.local_files = nucliadb_args.blob
    if nucliadb_args.key is None:
        ingest_settings.pull_time = 0
        nuclia_settings.disable_send_to_process = True
    else:
        nuclia_settings.nuclia_service_account = nucliadb_args.key
    nuclia_settings.onprem = True
    nuclia_settings.nuclia_zone = nucliadb_args.zone
    if nucliadb_args.host:
        nuclia_settings.nuclia_public_url = nucliadb_args.host
    nucliadb_settings.nucliadb_ingest = None
    transaction_settings.transaction_local = True
    audit_settings.audit_driver = "basic"
    indexing_settings.index_local = True
    cache_settings.cache_enabled = False
    writer_settings.dm_enabled = False
    ingest_settings.grpc_port = nucliadb_args.grpc

    os.environ["DATA_PATH"] = nucliadb_args.node

    local_node = LocalNode()
    NODE_CLUSTER.local_node = local_node


def run_nucliadb(nucliadb_args: Settings):
    from nucliadb_one.app import application
    from nucliadb_utils.settings import running_settings

    uvicorn.run(
        application,
        host="0.0.0.0",
        port=nucliadb_args.http,
        log_config=log_config,
        log_level=logging.getLevelName(running_settings.log_level),
        debug=True,
        reload=False,
    )
    logger.info(f"======= REST API on http://0.0.0.0:{nucliadb_args.http}/ ======")


async def run_async_nucliadb(nucliadb_args: Settings):
    from nucliadb_one.app import application
    from nucliadb_utils.settings import running_settings

    config = uvicorn.Config(
        application,
        port=nucliadb_args.http,
        log_level=logging.getLevelName(running_settings.log_level),
    )
    server = uvicorn.Server(config)
    config.load()
    server.lifespan = config.lifespan_class(config)
    await server.startup()
    return server


def purge():
    from nucliadb_ingest.settings import settings as ingest_settings
    from nucliadb_writer.settings import settings as writer_settings
    from nucliadb_utils.settings import (
        running_settings,
        storage_settings,
        nuclia_settings,
        nucliadb_settings,
        transaction_settings,
        audit_settings,
        indexing_settings,
    )
    from nucliadb_utils.cache.settings import settings as cache_settings

    nucliadb_args = arg_parse()

    ingest_settings.driver = "local"
    ingest_settings.driver_local_url = nucliadb_args.maindb
    ingest_settings.partitions = ["1"]
    running_settings.debug = True
    storage_settings.file_backend = "local"
    storage_settings.local_files = nucliadb_args.blob
    nuclia_settings.onprem = True
    nucliadb_settings.nucliadb_ingest = None
    transaction_settings.transaction_local = True
    audit_settings.audit_driver = "basic"
    indexing_settings.index_local = True
    cache_settings.cache_enabled = False
    writer_settings.dm_enabled = False

    local_node = LocalNode()
    NODE_CLUSTER.local_node = local_node

    asyncio.run(main())
