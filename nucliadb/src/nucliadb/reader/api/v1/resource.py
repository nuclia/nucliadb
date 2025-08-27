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
from typing import Optional, Union

from fastapi import Header, HTTPException, Query, Request, Response
from fastapi_versioning import version

from nucliadb.common.datamanagers.resources import KB_RESOURCE_SLUG_BASE
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.models_utils import from_proto, to_proto
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as ORMKnowledgeBox
from nucliadb.ingest.orm.resource import Resource as ORMResource
from nucliadb.ingest.serialize import (
    managed_serialize,
    serialize,
    set_resource_field_extracted_data,
)
from nucliadb.reader import SERVICE_NAME
from nucliadb.reader.api import DEFAULT_RESOURCE_LIST_PAGE_SIZE
from nucliadb.reader.api.models import (
    FIELD_NAME_TO_EXTRACTED_DATA_FIELD_MAP,
    ResourceField,
)
from nucliadb.reader.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import (
    Error,
    ExtractedDataTypeName,
    NucliaDBRoles,
    Resource,
    ResourceFieldProperties,
    ResourceList,
    ResourcePagination,
)
from nucliadb_models.search import ResourceProperties
from nucliadb_protos import resources_pb2, writer_pb2
from nucliadb_protos.writer_pb2 import FieldStatus
from nucliadb_telemetry import errors
from nucliadb_utils.authentication import requires, requires_one
from nucliadb_utils.utilities import get_audit, get_storage


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/resources",
    status_code=200,
    description="List of resources of a knowledgebox",
    tags=["Resources"],
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def list_resources(
    request: Request,
    response: Response,
    kbid: str,
    page: int = Query(0, description="Requested page number (0-based)"),
    size: int = Query(DEFAULT_RESOURCE_LIST_PAGE_SIZE, description="Page size"),
) -> ResourceList:
    # Get all resource id's fast by scanning all existing slugs

    # Get counters from maindb
    driver = get_driver()
    async with driver.ro_transaction() as txn:
        # Filter parameters for serializer
        show: list[ResourceProperties] = [ResourceProperties.BASIC]
        field_types: list[FieldTypeName] = []
        extracted: list[ExtractedDataTypeName] = []

        try:
            resources: list[Resource] = []
            max_items_to_iterate = (page + 1) * size
            first_wanted_item_index = (page * size) + 1  # 1-based index
            current_key_index = 0

            # ask for one item more than we need, in order to know if it's the last page
            keys_generator = txn.keys(
                match=KB_RESOURCE_SLUG_BASE.format(kbid=kbid),
                count=max_items_to_iterate + 1,
            )
            async for key in keys_generator:
                current_key_index += 1

                # First of all, we need to skip keys, in case we are on a +1 page
                if page > 0 and current_key_index < first_wanted_item_index:
                    continue

                # Don't fetch keys once we got all items for this
                if len(resources) == size:
                    await keys_generator.aclose()
                    break

                # Fetch and Add wanted item
                rid = await txn.get(key, for_update=False)
                if rid:
                    result = await managed_serialize(
                        txn,
                        kbid,
                        rid.decode(),
                        show,
                        field_types,
                        extracted,
                        service_name=SERVICE_NAME,
                    )
                    if result is not None:
                        resources.append(result)

            is_last_page = current_key_index <= max_items_to_iterate

        except Exception as exc:
            errors.capture_exception(exc)
            raise HTTPException(status_code=500, detail="Couldn't retrieve list of resources right now")

    return ResourceList(
        resources=resources,
        pagination=ResourcePagination(page=page, size=size, last=is_last_page),
    )


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}",
    status_code=200,
    summary="Get Resource (by id)",
    response_model=Resource,
    response_model_exclude_unset=True,
    tags=["Resources"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_resource_by_uuid(
    request: Request,
    kbid: str,
    rid: str,
    show: list[ResourceProperties] = Query([ResourceProperties.BASIC]),
    field_type_filter: list[FieldTypeName] = Query(list(FieldTypeName), alias="field_type"),
    extracted: list[ExtractedDataTypeName] = Query(
        [
            ExtractedDataTypeName.TEXT,
            ExtractedDataTypeName.METADATA,
            ExtractedDataTypeName.LINK,
            ExtractedDataTypeName.FILE,
        ]
    ),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
):
    return await _get_resource(
        rid=rid,
        kbid=kbid,
        show=show,
        field_type_filter=field_type_filter,
        extracted=extracted,
        x_nucliadb_user=x_nucliadb_user,
        x_forwarded_for=x_forwarded_for,
    )


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}",
    status_code=200,
    summary="Get Resource (by slug)",
    response_model=Resource,
    response_model_exclude_unset=True,
    tags=["Resources"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_resource_by_slug(
    request: Request,
    kbid: str,
    rslug: str,
    show: list[ResourceProperties] = Query([ResourceProperties.BASIC]),
    field_type_filter: list[FieldTypeName] = Query(list(FieldTypeName), alias="field_type"),
    extracted: list[ExtractedDataTypeName] = Query(
        [
            ExtractedDataTypeName.TEXT,
            ExtractedDataTypeName.METADATA,
            ExtractedDataTypeName.LINK,
            ExtractedDataTypeName.FILE,
        ]
    ),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> Resource:
    return await _get_resource(
        rslug=rslug,
        kbid=kbid,
        show=show,
        field_type_filter=field_type_filter,
        extracted=extracted,
        x_nucliadb_user=x_nucliadb_user,
        x_forwarded_for=x_forwarded_for,
    )


async def _get_resource(
    *,
    rslug: Optional[str] = None,
    rid: Optional[str] = None,
    kbid: str,
    show: list[ResourceProperties],
    field_type_filter: list[FieldTypeName],
    extracted: list[ExtractedDataTypeName],
    x_nucliadb_user: str,
    x_forwarded_for: str,
) -> Resource:
    if all([rslug, rid]) or not any([rslug, rid]):
        raise ValueError("Either rid or rslug must be provided, but not both")

    audit = get_audit()
    if audit is not None:
        audit_id = rid if rid else rslug
        audit.visited(kbid, audit_id, x_nucliadb_user, x_forwarded_for)  # type: ignore

    result = await serialize(
        kbid,
        rid,
        show,
        field_type_filter,
        extracted,
        service_name=SERVICE_NAME,
        slug=rslug,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="Resource does not exist")
    return result


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/{{field_type}}/{{field_id}}",
    status_code=200,
    summary="Get Resource field (by slug)",
    response_model=ResourceField,
    response_model_exclude_unset=True,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_resource_field_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_type: FieldTypeName,
    field_id: str,
    show: list[ResourceFieldProperties] = Query([ResourceFieldProperties.VALUE]),
    extracted: list[ExtractedDataTypeName] = Query(
        [
            ExtractedDataTypeName.TEXT,
            ExtractedDataTypeName.METADATA,
            ExtractedDataTypeName.LINK,
            ExtractedDataTypeName.FILE,
        ]
    ),
    # not working with latest pydantic/fastapi
    # page: Union[Literal["last", "first"], int] = Query("last"),
    page: Union[str, int] = Query("last"),
) -> Response:
    return await _get_resource_field(
        kbid,
        rslug=rslug,
        field_type=field_type,
        field_id=field_id,
        show=show,
        extracted=extracted,
        page=page,
    )


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/{{field_type}}/{{field_id}}",
    status_code=200,
    summary="Get Resource field (by id)",
    response_model=ResourceField,
    response_model_exclude_unset=True,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_resource_field_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_type: FieldTypeName,
    field_id: str,
    show: list[ResourceFieldProperties] = Query([ResourceFieldProperties.VALUE]),
    extracted: list[ExtractedDataTypeName] = Query(
        [
            ExtractedDataTypeName.TEXT,
            ExtractedDataTypeName.METADATA,
            ExtractedDataTypeName.LINK,
            ExtractedDataTypeName.FILE,
        ]
    ),
    # not working with latest pydantic/fastapi
    # page: Union[Literal["last", "first"], int] = Query("last"),
    page: Union[str, int] = Query("last"),
) -> Response:
    return await _get_resource_field(
        kbid,
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        show=show,
        extracted=extracted,
        page=page,
    )


async def _get_resource_field(
    kbid: str,
    field_type: FieldTypeName,
    field_id: str,
    show: list[ResourceFieldProperties],
    extracted: list[ExtractedDataTypeName],
    page: Union[str, int],
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> Response:
    storage = await get_storage(service_name=SERVICE_NAME)
    driver = get_driver()
    pb_field_id = to_proto.field_type_name(field_type)
    async with driver.ro_transaction() as txn:
        kb = ORMKnowledgeBox(txn, storage, kbid)

        if rid is None:
            assert rslug is not None, "Either rid or rslug must be defined"
            rid = await kb.get_resource_uuid_by_slug(rslug)
            if rid is None:
                raise HTTPException(status_code=404, detail="Resource does not exist")

        resource = ORMResource(txn, storage, kb, rid)
        field = await resource.get_field(field_id, pb_field_id, load=True)
        if field is None:
            raise HTTPException(status_code=404, detail="Knowledge Box does not exist")

        resource_field = ResourceField(field_id=field_id, field_type=field_type)

        if ResourceFieldProperties.VALUE in show:
            value = await field.get_value()

            if isinstance(value, resources_pb2.FieldText):
                value = await field.get_value()
                resource_field.value = from_proto.field_text(value)

            if isinstance(value, resources_pb2.FieldFile):
                value = await field.get_value()
                resource_field.value = from_proto.field_file(value)

            if isinstance(value, resources_pb2.FieldLink):
                value = await field.get_value()
                resource_field.value = from_proto.field_link(value)

            if isinstance(field, Conversation):
                if page == "first":
                    page_to_fetch = 1
                elif page == "last":
                    conversation_metadata = await field.get_metadata()
                    page_to_fetch = conversation_metadata.pages
                else:
                    page_to_fetch = int(page)

                value = await field.get_value(page=page_to_fetch)
                if value is not None:
                    resource_field.value = from_proto.conversation(value)

        if ResourceFieldProperties.EXTRACTED in show and extracted:
            resource_field.extracted = FIELD_NAME_TO_EXTRACTED_DATA_FIELD_MAP[field_type]()
            await set_resource_field_extracted_data(
                field,
                resource_field.extracted,
                field_type,
                extracted,
            )

        if ResourceFieldProperties.ERROR in show:
            status = await field.get_status()
            if status is None:
                status = FieldStatus()
            resource_field.status = status.Status.Name(status.status)
            if status.errors:
                resource_field.errors = []
                for error in status.errors:
                    resource_field.errors.append(
                        Error(
                            body=error.source_error.error,
                            code=error.source_error.code,
                            code_str=writer_pb2.Error.ErrorCode.Name(error.source_error.code),
                            created=error.created.ToDatetime(),
                            severity=writer_pb2.Error.Severity.Name(error.source_error.severity),
                        )
                    )
                resource_field.error = resource_field.errors[-1]

    return Response(
        content=resource_field.model_dump_json(exclude_unset=True, by_alias=True),
        media_type="application/json",
    )
