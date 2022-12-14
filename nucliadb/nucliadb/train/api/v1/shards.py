from fastapi import HTTPException, Request, Response
from nucliadb.train.api.utils import (
    create_response_dict,
    get_kb_partitions,
    get_train,
    stream_data_from_trainset,
)
from nucliadb.train.generator import generate_train_data
from nucliadb.train.utils import get_processor
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxResponseStatus
from nucliadb_utils.authentication import requires_one
from nucliadb_models.resource import (
    NucliaDBRoles,
)
from nucliadb.train.api.v1.router import KB_PREFIX, api
from fastapi.responses import StreamingResponse


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/trainset/{{shard}}",
    tags=["Object Response"],
    status_code=200,
    name="Return S3 Object call",
)
@requires_one([NucliaDBRoles.READER])
async def object_get_response(
    request: Request, kbid: str, shard: str, item: str
) -> StreamingResponse:
    proc = get_processor()
    train = get_train(item)
    response = await proc.get_kb(uuid=train.kbid)
    if response.status == KnowledgeBoxResponseStatus.NOTFOUND:
        raise HTTPException(status_code=404)

    all_keys = await get_kb_partitions(kbid, shard)

    if len(all_keys) == 0:
        raise HTTPException(status_code=404)

    return StreamingResponse(
        generate_train_data(train), media_type="application/octet-stream"
    )
