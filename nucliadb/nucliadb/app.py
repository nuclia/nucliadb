from nucliadb import config_nucliadb
from nucliadb import Settings

nucliadb_args = Settings()
config_nucliadb(nucliadb_args)

from nucliadb_one.app import application  # noqa