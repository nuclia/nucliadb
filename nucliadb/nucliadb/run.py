from nucliadb import Settings, config_nucliadb, run_nucliadb

if __name__ == "__main__":
    nucliadb_args = Settings()
    config_nucliadb(nucliadb_args)
    run_nucliadb(nucliadb_args)
