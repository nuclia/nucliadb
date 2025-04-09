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

from fastapi import HTTPException, Response
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb import learning_proxy
from nucliadb.common import datamanagers
from nucliadb.ingest.orm.exceptions import VectorSetConflict
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.writer import logger
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import (
    NucliaDBRoles,
)
from nucliadb_models.vectorsets import CreatedVectorSet
from nucliadb_protos import knowledgebox_pb2
from nucliadb_telemetry import errors
from nucliadb_utils.authentication import requires_one
from nucliadb_utils.utilities import get_storage


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/vectorsets/{{vectorset_id}}",
    status_code=201,
    summary="Add a vector set to Knowledge Box",
    tags=["VectorSets"],
    # TODO: remove when the feature is mature
    include_in_schema=False,
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def add_vectorset(request: Request, kbid: str, vectorset_id: str) -> CreatedVectorSet:
    try:
        await _add_vectorset(kbid, vectorset_id)

    except learning_proxy.ProxiedLearningConfigError as err:
        raise HTTPException(
            status_code=err.status_code,
            detail=err.content,
        )

    except VectorSetConflict as err:
        raise HTTPException(
            status_code=409,
            detail=str(err),
        )

    return CreatedVectorSet(id=vectorset_id)


async def _add_vectorset(kbid: str, vectorset_id: str) -> None:
    storage = await get_storage()

    async with datamanagers.with_ro_transaction() as txn:
        kbobj = KnowledgeBox(txn, storage, kbid)
        if await kbobj.vectorset_marked_for_deletion(vectorset_id):
            raise VectorSetConflict("Vectorset is already being deleted. Please try again later.")

    # First off, add the vectorset to the learning configuration if it's not already there
    lconfig = await learning_proxy.get_configuration(kbid)
    assert lconfig is not None
    semantic_models = lconfig.model_dump()["semantic_models"]
    if vectorset_id not in semantic_models:
        semantic_models.append(vectorset_id)
        await learning_proxy.update_configuration(kbid, {"semantic_models": semantic_models})
        lconfig = await learning_proxy.get_configuration(kbid)
        assert lconfig is not None

    # Then, add the vectorset to the index if it's not already there
    vectorset_config = get_vectorset_config(lconfig, vectorset_id)
    async with datamanagers.with_rw_transaction() as txn:
        kbobj = KnowledgeBox(txn, storage, kbid)
        await kbobj.create_vectorset(vectorset_config)
        await txn.commit()


def get_vectorset_config(
    learning_config: learning_proxy.LearningConfiguration, vectorset_id: str
) -> knowledgebox_pb2.VectorSetConfig:
    """
    Create a VectorSetConfig from a LearningConfiguration for a given vectorset_id
    """
    vectorset_config = knowledgebox_pb2.VectorSetConfig(vectorset_id=vectorset_id)
    vectorset_index_config = knowledgebox_pb2.VectorIndexConfig(
        vector_type=knowledgebox_pb2.VectorType.DENSE_F32,
    )
    model_config = learning_config.semantic_model_configs[vectorset_id]

    # Parse similarity function
    parsed_similarity = learning_proxy.SimilarityFunction(model_config.similarity)
    if parsed_similarity == learning_proxy.SimilarityFunction.COSINE.value:
        vectorset_index_config.similarity = knowledgebox_pb2.VectorSimilarity.COSINE
    elif parsed_similarity == learning_proxy.SimilarityFunction.DOT.value:
        vectorset_index_config.similarity = knowledgebox_pb2.VectorSimilarity.DOT
    else:
        raise ValueError(
            f"Unknown similarity function {model_config.similarity}, parsed as {parsed_similarity}"
        )

    # Parse vector dimension
    vectorset_index_config.vector_dimension = model_config.size

    # Parse matryoshka dimensions
    if len(model_config.matryoshka_dims) > 0:
        vectorset_index_config.normalize_vectors = True
        vectorset_config.matryoshka_dimensions.extend(model_config.matryoshka_dims)
    else:
        vectorset_index_config.normalize_vectors = False
    vectorset_config.vectorset_index_config.CopyFrom(vectorset_index_config)
    return vectorset_config


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/vectorsets/{{vectorset_id}}",
    status_code=204,
    summary="Delete vector set from Knowledge Box",
    tags=["VectorSets"],
    # TODO: remove when the feature is mature
    include_in_schema=False,
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.WRITER])
@version(1)
async def delete_vectorset(request: Request, kbid: str, vectorset_id: str) -> Response:
    try:
        await _delete_vectorset(kbid, vectorset_id)

    except VectorSetConflict as exc:
        raise HTTPException(
            status_code=409,
            detail=str(exc),
        )

    except learning_proxy.ProxiedLearningConfigError as err:
        raise HTTPException(
            status_code=err.status_code,
            detail=err.content,
        )

    return Response(status_code=204)


async def _delete_vectorset(kbid: str, vectorset_id: str) -> None:
    lconfig = await learning_proxy.get_configuration(kbid)
    if lconfig is not None:
        semantic_models = lconfig.model_dump()["semantic_models"]
        if vectorset_id in semantic_models:
            semantic_models.remove(vectorset_id)
            await learning_proxy.update_configuration(kbid, {"semantic_models": semantic_models})

    storage = await get_storage()
    try:
        async with datamanagers.with_rw_transaction() as txn:
            kbobj = KnowledgeBox(txn, storage, kbid)
            await kbobj.delete_vectorset(vectorset_id=vectorset_id)
            await txn.commit()

    except VectorSetConflict:
        # caller should handle this error
        raise
    except Exception as ex:
        errors.capture_exception(ex)
        logger.exception(
            "Could not delete vectorset from index", extra={"kbid": kbid, "vectorset_id": vectorset_id}
        )
