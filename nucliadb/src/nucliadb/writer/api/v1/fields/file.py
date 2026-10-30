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
from typing import Annotated

from fastapi import HTTPException
from fastapi_versioning import version
from starlette.requests import Request

import nucliadb_models
from nucliadb.common.back_pressure import maybe_back_pressure
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.file import File
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.models.internal.processing import PushPayload, Source
from nucliadb.writer import SERVICE_NAME, transaction
from nucliadb.writer.api.constants import X_SKIP_STORE
from nucliadb.writer.api.v1.field import add_field_to_resource, add_field_to_resource_by_slug
from nucliadb.writer.api.v1.resource import (
    get_rid_from_slug_or_raise_error,
    validate_rid_exists_or_raise_error,
)
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api
from nucliadb.writer.resource.audit import parse_audit
from nucliadb.writer.resource.field import (
    collect_fields_for_reprocessing,
)
from nucliadb.writer.utilities import get_processing
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.utils import FieldIdString
from nucliadb_models.writer import ResourceFieldAdded, ResourceFieldUpdated
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage, FieldIDStatus, FieldStatus
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError, SendToProcessError
from nucliadb_utils.utilities import get_partitioning, get_storage


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field_id}}",
    status_code=201,
    summary="Add resource file field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_file_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    field_payload: nucliadb_models.FileField,
    x_skip_store: Annotated[bool, X_SKIP_STORE] = False,
) -> ResourceFieldAdded:
    return await add_field_to_resource_by_slug(
        request, kbid, rslug, field_id, field_payload, skip_store=x_skip_store
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field_id}}",
    status_code=201,
    summary="Add resource file field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_file_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: nucliadb_models.FileField,
    x_skip_store: Annotated[bool, X_SKIP_STORE] = False,
) -> ResourceFieldAdded:
    return await add_field_to_resource(
        request, kbid, rid, field_id, field_payload, skip_store=x_skip_store
    )


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field_id}}",
    status_code=200,
    summary="Update resource file field (by slug)",
    response_model=ResourceFieldUpdated,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def update_resource_field_file_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    update: nucliadb_models.UpdateFileField,
) -> ResourceFieldUpdated:
    rid = await get_rid_from_slug_or_raise_error(kbid, rslug)
    return await update_and_reprocess_file(request, kbid, rid, field_id, update)


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field_id}}",
    status_code=200,
    summary="Update resource file field (by id)",
    response_model=ResourceFieldUpdated,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def update_resource_field_file_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    update: nucliadb_models.UpdateFileField,
) -> ResourceFieldUpdated:
    await validate_rid_exists_or_raise_error(kbid, rid)
    return await update_and_reprocess_file(request, kbid, rid, field_id, update)


async def update_and_reprocess_file(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    update: nucliadb_models.UpdateFileField,
) -> ResourceFieldUpdated:
    await maybe_back_pressure(kbid, resource_uuid=rid)

    partitioning = get_partitioning()
    partition = partitioning.generate_partition(kbid, rid)

    driver = get_driver()
    storage = await get_storage(service_name=SERVICE_NAME)

    async with driver.ro_transaction() as txn:
        kb = KnowledgeBox(txn, storage, kbid)

        resource = await kb.get(rid)
        if resource is None:
            raise HTTPException(status_code=404, detail="Resource does not exist")

        field: File = await resource.get_field(field_id, FieldType.FILE, load=True)
        file_field_pb = await field.get_value()
        if file_field_pb is None:
            raise HTTPException(status_code=404, detail="File field does not exist")

        writer = BrokerMessage()
        toprocess = PushPayload(
            uuid=rid,
            kbid=kbid,
            partition=partition,
            userid=request.headers.get("X-NUCLIADB-USER", ""),
        )

        writer.kbid = kbid
        writer.uuid = rid
        toprocess.kbid = kbid
        toprocess.uuid = rid
        toprocess.source = Source.HTTP
        if resource.basic is not None:
            toprocess.title = resource.basic.title
            toprocess.slug = resource.basic.slug

        parse_audit(writer.audit, request)

        if update.extract_strategy is None:
            file_field_pb.ClearField("extract_strategy")
        else:
            file_field_pb.extract_strategy = update.extract_strategy
        writer.files[field_id].CopyFrom(file_field_pb)

        # set processing payload from updated field
        await collect_fields_for_reprocessing(
            resource,
            toprocess,
            selected_fields={(FieldType.FILE, field_id)},
        )

        writer.field_statuses.append(
            FieldIDStatus(
                id=resources_pb2.FieldID(field_type=resources_pb2.FieldType.FILE, field=field_id),
                status=FieldStatus.Status.PENDING,
            )
        )

    processing = get_processing()
    await transaction.commit(writer, partition)
    try:
        processing_info = await processing.send_to_process(toprocess, partition)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(
            status_code=500,
            detail="Error while sending to process. Try calling /reprocess",
        )
    seqid = processing_info.seqid
    return nucliadb_models.ResourceFieldUpdated(seqid=seqid)
