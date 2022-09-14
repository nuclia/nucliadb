from nucliadb import config_nucliadb
from nucliadb import run_nucliadb
from nucliadb import Settings
from line_profiler import LineProfiler


if __name__ == "__main__":
    nucliadb_args = Settings()
    config_nucliadb(nucliadb_args)
    run_nucliadb(nucliadb_args)
