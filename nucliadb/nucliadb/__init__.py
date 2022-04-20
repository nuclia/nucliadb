import logging
import argparse

import uvicorn

logger = logging.getLogger("nucliadb")


def arg_parse():

    parser = argparse.ArgumentParser(description="Process some integers.")
    parser.add_argument(
        "-p",
        "--maindb",
        dest="maindb",
        help="MainDB data folder",
    )

    parser.add_argument(
        "-b",
        "--blobstorage",
        dest="blob",
        help="Blob data folder",
    )

    parser.add_argument(
        "-k",
        "--key",
        dest="key",
        help="Understanding API Key",
    )

    parser.add_argument(
        "-s",
        "--stream",
        dest="kestreamy",
        help="Stream data folder",
    )

    parser.add_argument("-z", "--zone", dest="zone", help="Understanding API Zone")

    args = parser.parse_args()
    return args


def run():
    from nucliadb_ingest.settings import settings as ingest_settings
    from nucliadb_search.settings import settings
    from nucliadb_utils.settings import (
        running_settings,
        http_settings,
        storage_settings,
        nuclia_settings,
        nucliadb_settings,
        transaction_settings,
    )

    nucliadb_args = arg_parse()

    ingest_settings.driver = "local"
    ingest_settings.driver_local_url = nucliadb_args.maindb
    running_settings.debug = True
    http_settings.cors_origins = ["*"]
    storage_settings.file_backend = "local"
    storage_settings.local_files = nucliadb_args.blob
    nuclia_settings.zone_key = nucliadb_args.key
    nucliadb_settings.nucliadb_ingest = None
    transaction_settings.transaction_local = True
    transaction_settings.transaction_local_path = f"{nucliadb_args.stream}/transaction"

    uvicorn.run(
        "nucliadb_one.app:application", host="127.0.0.1", port=8080, log_level="info"
    )
