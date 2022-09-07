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
import os
from enum import Enum
from typing import Optional

import pydantic
import pydantic_argparse
import uvicorn  # type: ignore

from nucliadb_ingest.orm import NODE_CLUSTER
from nucliadb_ingest.orm.local_node import LocalNode
from nucliadb_ingest.purge import main

logger = logging.getLogger("nucliadb")


# this is default (site-packages\uvicorn\main.py)
log_config = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {
            "()": "uvicorn.logging.DefaultFormatter",
            "fmt": "%(asctime)s %(levelprefix)s %(message)s",
            "use_colors": None,
        },
        "access": {
            "()": "uvicorn.logging.AccessFormatter",
            "fmt": '%(asctime)s %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
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
            "propagate": False,
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


class LogLevel(str, Enum):
    INFO = "INFO"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class Driver(str, Enum):
    REDIS = "REDIS"
    LOCAL = "LOCAL"


class Settings(pydantic.BaseSettings):
    driver: Driver = pydantic.Field(Driver.LOCAL, description="Main DB Path string")
    maindb: str = pydantic.Field(description="Main DB Path string")
    blob: str = pydantic.Field(description="Blob Path string")
    key: Optional[str] = pydantic.Field(
        description="Nuclia Understanding API Key string"
    )
    node: str = pydantic.Field(description="Node Path string")
    zone: Optional[str] = pydantic.Field(description="Nuclia Understanding API Zone ID")
    http: int = pydantic.Field(8080, description="HTTP Port int")
    grpc: int = pydantic.Field(8030, description="GRPC Port int")
    train: int = pydantic.Field(8031, description="Train GRPC Port int")
    log: LogLevel = pydantic.Field(
        LogLevel.ERROR, description="Log level [DEBUG,INFO,ERROR] string"
    )


def run():
    if os.environ.get("NUCLIADB_ENV"):
        nucliadb_args = Settings()
    else:

        parser = pydantic_argparse.ArgumentParser(
            model=Settings,
            prog="NucliaDB",
            description="NucliaDB Starting script",
        )
        nucliadb_args = parser.parse_typed_args()

    config_nucliadb(nucliadb_args)
    run_nucliadb(nucliadb_args)


def config_nucliadb(nucliadb_args: Settings):
    from nucliadb_ingest.settings import settings as ingest_settings
    from nucliadb_train.settings import settings as train_settings
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
    from nucliadb_writer.settings import settings as writer_settings

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
    elif nucliadb_args.driver == Driver.REDIS:
        ingest_settings.driver = "redis"
        ingest_settings.driver_redis_url = nucliadb_args.maindb

    storage_settings.file_backend = "local"
    storage_settings.local_files = nucliadb_args.blob

    os.environ["DATA_PATH"] = nucliadb_args.node

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


async def run_async_nucliadb(nucliadb_args: Settings):
    from nucliadb_one.app import application
    from nucliadb_utils.settings import running_settings

    config = uvicorn.Config(
        application,
        port=nucliadb_args.http,
        log_level=logging.getLevelName(running_settings.log_level),
        log_config=log_config,
    )
    server = uvicorn.Server(config)
    config.load()
    server.lifespan = config.lifespan_class(config)
    await server.startup()
    return server


def purge():
    from nucliadb_ingest.settings import settings as ingest_settings
    from nucliadb_utils.cache.settings import settings as cache_settings
    from nucliadb_utils.settings import (
        audit_settings,
        indexing_settings,
        nuclia_settings,
        nucliadb_settings,
        running_settings,
        storage_settings,
        transaction_settings,
    )
    from nucliadb_writer.settings import settings as writer_settings

    if os.environ.get("NUCLIADB_ENV"):
        nucliadb_args = Settings()
    else:

        parser = pydantic_argparse.ArgumentParser(
            model=Settings,
            prog="NucliaDB",
            description="NucliaDB Starting script",
        )
        nucliadb_args = parser.parse_typed_args()

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
