from nucliadb_search.app import application
from nucliadb_utils.fastapi.run import run_fastapi_with_metrics


def run_with_metrics():
    run_fastapi_with_metrics(application)
