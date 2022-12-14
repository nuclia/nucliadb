from fastapi_versioning import version  # type: ignore
from fastapi import Request
from nucliadb.train.api.models import TrainSetPartitions
from nucliadb.train.api.utils import get_kb_partitions
from nucliadb_utils.authentication import requires_one
from nucliadb_models.resource import (
    NucliaDBRoles,
)
from nucliadb.train.api.v1.router import KB_PREFIX, api


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/trainset",
    tags=["Train"],
    status_code=200,
    name="Return Train call",
    response_model=TrainSetPartitions,
)
@api.get(
    f"/{KB_PREFIX}/{{kbid}}/trainset/{{prefix}}",
    tags=["Train"],
    status_code=200,
    name="Return Train call",
    response_model=TrainSetPartitions,
)
@version(1)
@requires_one([NucliaDBRoles.READER])
async def get_partitions(
    request: Request, kbid: str, prefix: str
) -> TrainSetPartitions:
    all_keys = await get_kb_partitions(kbid, prefix)
    return TrainSetPartitions(partitions=all_keys)
