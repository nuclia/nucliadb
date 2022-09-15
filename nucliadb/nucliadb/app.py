from nucliadb import Settings, config_nucliadb

nucliadb_args = Settings()
config_nucliadb(nucliadb_args)

from nucliadb_one.app import application  # noqa
