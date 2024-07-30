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
import asyncio
from functools import partial, wraps
from typing import Optional

from fastapi import HTTPException
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb import learning_proxy
from nucliadb.common import datamanagers
from nucliadb.common.external_index_providers.exceptions import ExternalIndexCreationError
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.writer import logger
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX, api
from nucliadb.writer.utilities import get_processing
from nucliadb_models.external_index_providers import (
    ExternalIndexProviderType,
    PineconeServerlessCloud,
)
from nucliadb_models.resource import (
    KnowledgeBoxConfig,
    KnowledgeBoxObj,
    KnowledgeBoxObjID,
    NucliaDBRoles,
)
from nucliadb_protos import knowledgebox_pb2
from nucliadb_utils.authentication import requires
from nucliadb_utils.settings import is_onprem_nucliadb


def only_for_onprem(fun):
    @wraps(fun)
    async def endpoint_wrapper(*args, **kwargs):
        if not is_onprem_nucliadb():
            raise HTTPException(
                status_code=403,
                detail="This endpoint is only available for onprem NucliaDB",
            )
        return await fun(*args, **kwargs)

    return endpoint_wrapper


@only_for_onprem
@api.post(
    f"/{KBS_PREFIX}",
    status_code=201,
    summary="Create Knowledge Box",
    tags=["Knowledge Boxes"],
    openapi_extra={"x-hidden-operation": True},
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def create_kb(request: Request, item: KnowledgeBoxConfig) -> KnowledgeBoxObj:
    try:
        kbid, slug = await _create_kb(item)
    except KnowledgeBoxConflict:
        raise HTTPException(status_code=419, detail="Knowledge box already exists")
    except ExternalIndexCreationError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    except Exception:
        raise HTTPException(status_code=500, detail="Error creating knowledge box")
    else:
        return KnowledgeBoxObj(uuid=kbid, slug=slug)


async def _create_kb(item: KnowledgeBoxConfig) -> tuple[str, Optional[str]]:
    driver = get_driver()
    rollback_learning_config = None

    kbid = KnowledgeBox.new_unique_kbid()

    # Onprem KBs have to call learning proxy to create it's own configuration.
    if item.learning_configuration:
        user_learning_config = item.learning_configuration
    else:
        logger.warning(
            "No learning configuration provided. Default will be used.",
            extra={"kbid": kbid},
        )
        # learning will choose the default values
        user_learning_config = {}

    # we rely on learning to return the updated configuration with defaults and
    # any other needed values (e.g. matryoshka settings if available)
    learning_config = await learning_proxy.set_configuration(kbid, config=user_learning_config)

    # if KB creation fails, we'll have to delete its learning config
    async def _rollback_learning_config(kbid: str):
        try:
            await learning_proxy.delete_configuration(kbid)
        except Exception:
            logger.warning(
                "Could not rollback learning configuration",
                exc_info=True,
                extra={"kbid": kbid},
            )

    rollback_learning_config = partial(_rollback_learning_config, kbid)

    semantic_model = learning_config.into_semantic_model_metadata()
    release_channel = item.release_channel.to_pb() if item.release_channel is not None else None

    external_index_provider = knowledgebox_pb2.CreateExternalIndexProviderMetadata(
        type=knowledgebox_pb2.ExternalIndexProviderType.UNSET,
    )
    if (
        item.external_index_provider
        and item.external_index_provider.type == ExternalIndexProviderType.PINECONE
    ):
        pinecone_api_key = item.external_index_provider.api_key
        serverless_pb = to_pinecone_serverless_cloud_pb(item.external_index_provider.serverless_cloud)
        external_index_provider = knowledgebox_pb2.CreateExternalIndexProviderMetadata(
            type=knowledgebox_pb2.ExternalIndexProviderType.PINECONE,
            pinecone_config=knowledgebox_pb2.CreatePineconeConfig(
                api_key=pinecone_api_key,
                serverless_cloud=serverless_pb,
            ),
        )

    try:
        (kbid, slug) = await KnowledgeBox.create(
            driver,
            kbid=kbid,
            slug=item.slug or kbid,
            title=item.title or "",
            description=item.description or "",
            semantic_model=semantic_model,
            release_channel=release_channel,
            external_index_provider=external_index_provider,
        )

    except Exception as exc:
        logger.error("Unexpected error creating KB", exc_info=exc, extra={"slug": item.slug})
        await rollback_learning_config()
        raise

    return (kbid, slug)


@only_for_onprem
@api.patch(
    f"/{KB_PREFIX}/{{kbid}}",
    status_code=200,
    summary="Update Knowledge Box",
    response_model=KnowledgeBoxObjID,
    tags=["Knowledge Boxes"],
    openapi_extra={"x-hidden-operation": True},
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def update_kb(request: Request, kbid: str, item: KnowledgeBoxConfig) -> KnowledgeBoxObjID:
    driver = get_driver()
    config = None
    if item.slug or item.title or item.description:
        config = knowledgebox_pb2.KnowledgeBoxConfig(
            slug=item.slug or "",
            title=item.title or "",
            description=item.description or "",
        )
    try:
        async with driver.transaction() as txn:
            await KnowledgeBox.update(
                txn,
                uuid=kbid,
                slug=item.slug,
                config=config,
            )
            await txn.commit()
    except datamanagers.exceptions.KnowledgeBoxNotFound:
        raise HTTPException(status_code=404, detail="Knowledge box does not exist")
    except Exception as exc:
        logger.exception("Could not update KB", exc_info=exc, extra={"kbid": kbid})
        raise HTTPException(status_code=500, detail="Error updating knowledge box")
    else:
        return KnowledgeBoxObjID(uuid=kbid)


@only_for_onprem
@api.delete(
    f"/{KB_PREFIX}/{{kbid}}",
    status_code=200,
    summary="Delete Knowledge Box",
    tags=["Knowledge Boxes"],
    openapi_extra={"x-hidden-operation": True},
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def delete_kb(request: Request, kbid: str) -> KnowledgeBoxObj:
    driver = get_driver()
    try:
        await KnowledgeBox.delete(driver, kbid=kbid)
    except datamanagers.exceptions.KnowledgeBoxNotFound:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exists")
    except Exception as exc:
        logger.exception("Could not delete KB", exc_info=exc, extra={"kbid": kbid})
        raise HTTPException(status_code=500, detail="Error deleting knowledge box")

    # onprem nucliadb must delete its learning configuration
    try:
        await learning_proxy.delete_configuration(kbid)
        logger.info("Learning configuration deleted", extra={"kbid": kbid})
    except Exception as exc:
        logger.exception(
            "Unexpected error deleting learning configuration",
            exc_info=exc,
            extra={"kbid": kbid},
        )

    # be nice and notify processing this KB is being deleted so we waste
    # resources
    processing = get_processing()
    asyncio.create_task(processing.delete_from_processing(kbid=kbid))

    return KnowledgeBoxObj(uuid=kbid)


def to_pinecone_serverless_cloud_pb(
    serverless: PineconeServerlessCloud,
) -> knowledgebox_pb2.PineconeServerlessCloud.ValueType:
    return {
        PineconeServerlessCloud.AWS_EU_WEST_1: knowledgebox_pb2.PineconeServerlessCloud.AWS_EU_WEST_1,
        PineconeServerlessCloud.AWS_US_EAST_1: knowledgebox_pb2.PineconeServerlessCloud.AWS_US_EAST_1,
        PineconeServerlessCloud.AWS_US_WEST_2: knowledgebox_pb2.PineconeServerlessCloud.AWS_US_WEST_2,
        PineconeServerlessCloud.AZURE_EASTUS2: knowledgebox_pb2.PineconeServerlessCloud.AZURE_EASTUS2,
        PineconeServerlessCloud.GCP_US_CENTRAL1: knowledgebox_pb2.PineconeServerlessCloud.GCP_US_CENTRAL1,
    }[serverless]
