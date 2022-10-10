import asyncio
import os

import pydantic_argparse

from nucliadb.config import config_nucliadb
from nucliadb.settings import Settings


def purge():
    from nucliadb.ingest.purge import main

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
    asyncio.run(main())
