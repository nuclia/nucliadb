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
from fastapi import Request
from fastapi_versioning import version

from nucliadb.learning_proxy import learning_config_proxy
from nucliadb.models.responses import HTTPClientError
from nucliadb.reader.api.v1.router import KB_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires
from nucliadb_utils.settings import is_onprem_nucliadb


@api.get(
    path=f"/{KB_PREFIX}/{{kbid}}/models/{{model_id}}/{{filename:path}}",
    status_code=200,
    summary="Download the Knowledege Box model",
    description="Download the trained model or any other generated file as a result of a training task on a Knowledge Box.",  # noqa
    response_model=None,
    tags=["Models"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def download_model(
    request: Request,
    kbid: str,
    model_id: str,
    filename: str,
):
    return await learning_config_proxy(request, "GET", f"/download/{kbid}/model/{model_id}/{filename}")


@api.get(
    path=f"/{KB_PREFIX}/{{kbid}}/configuration",
    status_code=200,
    summary="Get Knowledge Box models configuration",
    description="Current configuration of models assigned to a Knowledge Box",
    response_model=None,
    tags=["Models"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_configuration(
    request: Request,
    kbid: str,
):
    return await learning_config_proxy(
        request,
        "GET",
        f"/config/{kbid}",
        extra_headers={"X-STF-USER": request.headers.get("X-NUCLIADB-USER", "")},
    )


@api.get(
    path=f"/{KB_PREFIX}/{{kbid}}/models",
    status_code=200,
    summary="Get available models",
    description="Get available models",
    response_model=None,
    tags=["Models"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_models(
    request: Request,
    kbid: str,
):
    return await learning_config_proxy(request, "GET", f"/models/{kbid}")


@api.get(
    path=f"/{KB_PREFIX}/{{kbid}}/model/{{model_id}}",
    status_code=200,
    summary="Get model metadata",
    description="Get metadata for a particular model",
    response_model=None,
    tags=["Models"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_model(
    request: Request,
    kbid: str,
    model_id: str,
):
    return await learning_config_proxy(
        request,
        "GET",
        f"/models/{kbid}/model/{model_id}",
        extra_headers={"X-STF-USER": request.headers.get("X-NUCLIADB-USER", "")},
    )


@api.get(
    path=f"/{KB_PREFIX}/{{kbid}}/schema",
    status_code=200,
    summary="Learning configuration schema",
    description="Get jsonschema definition to update the `learning_configuration` of your Knowledge Box",
    response_model=None,
    tags=["Models"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_schema_for_configuration_updates(
    request: Request,
    kbid: str,
):
    return await learning_config_proxy(request, "GET", f"/schema/{kbid}")


@api.get(
    path=f"/nua/schema",
    status_code=200,
    summary="Learning configuration schema for Knowledge Box creation",
    description="Get jsonschema definition for `learning_configuration` field for the Knowledge Box creation payload",
    response_model=None,
    tags=["Models"],
    include_in_schema=False,
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_schema_for_configuration_creation(
    request: Request,
):
    if not is_onprem_nucliadb():
        return HTTPClientError(status_code=404, detail="Endpoint not available for Hosted NucliaDB")
    return await learning_config_proxy(request, "GET", f"/schema")
