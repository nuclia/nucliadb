import prometheus_client  # type: ignore
from fastapi import FastAPI
from starlette.responses import PlainTextResponse

from nucliadb_utils.settings import running_settings


async def metrics(request):
    output = prometheus_client.exposition.generate_latest()
    return PlainTextResponse(output.decode("utf8"))


application_metrics = FastAPI(title="NucliaDB Metrics API", debug=running_settings.debug)  # type: ignore
application_metrics.add_route("/metrics", metrics)
