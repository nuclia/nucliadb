# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
import json
from typing import Optional

import google.protobuf.message
import pydantic
from fastapi import HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi_versioning import version

from nucliadb.common.cluster.exceptions import ShardNotFound
from nucliadb.train.api.utils import get_kb_partitions
from nucliadb.train.api.v1.router import KB_PREFIX, api
from nucliadb.train.generator import generate_train_data
from nucliadb_models.filters import FilterExpression
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.trainset import TrainSet as TrainSetModel
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet
from nucliadb_utils.authentication import requires_one


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/trainset/{{shard}}",
    tags=["Object Response"],
    status_code=200,
    summary="Return Train Stream",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def object_get_response(
    request: Request,
    kbid: str,
    shard: str,
) -> StreamingResponse:
    try:
        partitions = await get_kb_partitions(kbid, prefix=shard)
    except ShardNotFound:
        raise HTTPException(status_code=404, detail=f"No shards found for kb")
    if shard not in partitions:
        raise HTTPException(status_code=404, detail=f"Partition {shard} not found")
    trainset, filter_expression = await get_trainset(request)
    return StreamingResponse(
        generate_train_data(kbid, shard, trainset, filter_expression),
        media_type="application/octet-stream",
    )


async def get_trainset(request: Request) -> tuple[TrainSet, Optional[FilterExpression]]:
    if request.headers.get("Content-Type") == "application/json":
        try:
            trainset_model = TrainSetModel.model_validate(await request.json())
        except (pydantic.ValidationError, json.JSONDecodeError, ValueError) as err:
            raise HTTPException(status_code=422, detail=str(err))
        trainset_pb = TrainSet(
            type=TaskType.ValueType(trainset_model.type.value),
            batch_size=trainset_model.batch_size,
            exclude_text=trainset_model.exclude_text,
        )
        filter_expression = trainset_model.filter_expression
    else:
        # Legacy version of the endpoint where the encoded TrainSet protobuf is passed as request body.
        trainset_pb = TrainSet()
        try:
            trainset_pb.ParseFromString(await request.body())
        except google.protobuf.message.DecodeError as err:
            raise HTTPException(status_code=422, detail=str(err))
        # Filter expressions not supported on legacy version of the endpoint
        filter_expression = None
    return trainset_pb, filter_expression
