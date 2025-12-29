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
from functools import partial

from fastapi import BackgroundTasks, HTTPException
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb import learning_proxy
from nucliadb.common import datamanagers
from nucliadb.common.external_index_providers.exceptions import (
    ExternalIndexCreationError,
)
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.writer import logger
from nucliadb.writer.api.utils import only_for_onprem
from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX, api
from nucliadb.writer.utilities import get_processing
from nucliadb_models.resource import (
    KnowledgeBoxConfig,
    KnowledgeBoxObj,
    KnowledgeBoxObjID,
    NucliaDBRoles,
)
from nucliadb_protos import knowledgebox_pb2
from nucliadb_utils.authentication import requires


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
async def create_kb_endpoint(request: Request, item: KnowledgeBoxConfig) -> KnowledgeBoxObj:
    try:
        kbid, slug = await create_kb(item)
    except KnowledgeBoxConflict:
        raise HTTPException(status_code=419, detail="Knowledge box already exists")
    except ExternalIndexCreationError as exc:
        raise HTTPException(status_code=512, detail=str(exc))
    except Exception:
        logger.exception("Could not create KB")
        raise HTTPException(status_code=500, detail="Error creating knowledge box")
    else:
        return KnowledgeBoxObj(uuid=kbid, slug=slug)


async def create_kb(item: KnowledgeBoxConfig) -> tuple[str, str]:
    driver = get_driver()
    rollback_learning_config = None

    kbid = KnowledgeBox.new_unique_kbid()

    # Onprem KB creation doesn't have an existing learning configuration yet, so
    # we need to call learning proxy to create it
    if item.learning_configuration:
        user_learning_config = item.learning_configuration
    else:
        logger.warning(
            "No learning configuration provided. Default will be used.",
            extra={"kbid": kbid},
        )
        # learning will choose the default values
        user_learning_config = {}

    # We need to be backward compatible with the old "semantic_model" field where
    # only one semantic model was allowed.
    if "semantic_model" in user_learning_config:
        user_learning_config["semantic_models"] = [user_learning_config.pop("semantic_model")]

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
    semantic_models = learning_config.into_semantic_models_metadata()

    external_index_provider = knowledgebox_pb2.CreateExternalIndexProviderMetadata(
        type=knowledgebox_pb2.ExternalIndexProviderType.UNSET,
    )
    try:
        (kbid, slug) = await KnowledgeBox.create(
            driver,
            kbid=kbid,
            slug=item.slug or kbid,
            title=item.title or "",
            description=item.description or "",
            semantic_models=semantic_models,
            external_index_provider=external_index_provider,
            hidden_resources_enabled=item.hidden_resources_enabled,
            hidden_resources_hide_on_creation=item.hidden_resources_hide_on_creation,
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
    if (
        item.slug
        or item.title
        or item.description
        or item.hidden_resources_enabled
        or item.hidden_resources_hide_on_creation
    ):
        try:
            driver = get_driver()
            await KnowledgeBox.update(
                driver,
                kbid=kbid,
                slug=item.slug,
                title=item.title,
                description=item.description,
                hidden_resources_enabled=item.hidden_resources_enabled,
                hidden_resources_hide_on_creation=item.hidden_resources_hide_on_creation,
            )
        except datamanagers.exceptions.KnowledgeBoxNotFound:
            raise HTTPException(status_code=404, detail="Knowledge box does not exist")
        except Exception as exc:
            logger.exception("Could not update KB", exc_info=exc, extra={"kbid": kbid})
            raise HTTPException(status_code=500, detail="Error updating knowledge box")

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
async def delete_kb(request: Request, kbid: str, background: BackgroundTasks) -> KnowledgeBoxObj:
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
    background.add_task(processing.delete_from_processing, kbid=kbid)

    return KnowledgeBoxObj(uuid=kbid)
