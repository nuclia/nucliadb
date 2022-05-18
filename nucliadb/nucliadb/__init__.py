import asyncio
import logging
import argparse
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
        "-s", "--http", dest="http", default=8080, help="NucliaDB HTTP Port", type=int
    )

    parser.add_argument("--log", dest="log", default="INFO", help="LOG LEVEL")

    args = parser.parse_args()
    return args


def run():
    from nucliadb_ingest.settings import settings as ingest_settings
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
    from nucliadb_one.app import application

    nucliadb_args = arg_parse()

    running_settings.log_level = nucliadb_args.log.upper()
    ingest_settings.driver = "local"
    ingest_settings.driver_local_url = nucliadb_args.maindb
    ingest_settings.swim_enabled = False
    search_settings.swim_enabled = False
    running_settings.debug = True
    http_settings.cors_origins = ["*"]
    storage_settings.file_backend = "local"
    storage_settings.local_files = nucliadb_args.blob
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
    uvicorn.run(
        application,
        host="0.0.0.0",
        port=nucliadb_args.http,
        log_config=log_config,
        log_level=logging.getLevelName("INFO"),
        debug=True,
        reload=False,
    )
    logger.info(f"======= REST API on http://0.0.0.0:{nucliadb_args.http}/ ======")


def purge():
    from nucliadb_ingest.settings import settings as ingest_settings
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
