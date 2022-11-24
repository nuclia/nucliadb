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
from typing import List, Literal, Optional, Union
from typing import get_args as typing_get_args

from fastapi import Header, HTTPException, Query, Request, Response
from fastapi_versioning import version
from sentry_sdk import capture_exception

import nucliadb.models as models
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as ORMKnowledgeBox
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG_BASE
from nucliadb.ingest.orm.resource import Resource as ORMResource
from nucliadb.ingest.serialize import (
    ExtractedDataTypeName,
    ResourceFieldProperties,
    ResourceProperties,
    get_resource_uuid_by_slug,
    serialize,
    set_resource_field_extracted_data,
)
from nucliadb.ingest.utils import get_driver
from nucliadb.models.common import FieldTypeName
from nucliadb.models.resource import (
    Error,
    NucliaDBRoles,
    Resource,
    ResourceList,
    ResourcePagination,
)
from nucliadb.reader import SERVICE_NAME  # type: ignore
from nucliadb.reader.api import DEFAULT_RESOURCE_LIST_PAGE_SIZE
from nucliadb.reader.api.models import (
    FIELD_NAME_TO_EXTRACTED_DATA_FIELD_MAP,
    FIELD_NAMES_TO_PB_TYPE_MAP,
    ResourceField,
)
from nucliadb.reader.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api
from nucliadb_protos import resources_pb2
from nucliadb_utils.authentication import requires, requires_one
from nucliadb_utils.utilities import get_audit, get_cache, get_storage


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
    page: int = Query(0),
    size: int = Query(DEFAULT_RESOURCE_LIST_PAGE_SIZE),
) -> ResourceList:
    # Get all resource id's fast by scanning all existing slugs

    # Get counters from maindb
    driver = await get_driver()
    txn = await driver.begin()

    # Filter parameters for serializer
    show: List[ResourceProperties] = [
        ResourceProperties.BASIC,
        ResourceProperties.EXTRACTED,
    ]
    field_types: List[FieldTypeName] = [FieldTypeName.FILE]
    extracted: List[ExtractedDataTypeName] = [ExtractedDataTypeName.METADATA]

    try:
        resources: List[Resource] = []
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
            rid = await txn.get(key)
            if rid is not None:
                result = await serialize(
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
        capture_exception(exc)
        raise HTTPException(
            status_code=500, detail="Couldn't retrieve list of resources right now"
        )
    finally:
        await txn.abort()

    return ResourceList(
        resources=resources,
        pagination=ResourcePagination(page=page, size=size, last=is_last_page),
    )


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}",
    status_code=200,
    name="Get Resource (by slug)",
    response_model=Resource,
    response_model_exclude_unset=True,
    tags=["Resources"],
)
@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}",
    status_code=200,
    name="Get Resource (by id)",
    response_model=Resource,
    response_model_exclude_unset=True,
    tags=["Resources"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_resource(
    request: Request,
    kbid: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
    show: List[ResourceProperties] = Query([ResourceProperties.BASIC]),
    field_type_filter: List[FieldTypeName] = Query(
        list(FieldTypeName), alias="field_type"
    ),
    extracted: List[ExtractedDataTypeName] = Query(
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
    audit = get_audit()
    if audit is not None:
        audit_id = rid if rid else rslug
        await audit.visited(kbid, audit_id, x_nucliadb_user, x_forwarded_for)  # type: ignore

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


PageShortcuts = Literal["last", "first"]
PAGE_SHORTCUTS = typing_get_args(PageShortcuts)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/{{field_type}}/{{field_id}}",
    status_code=200,
    name="Get Resource field (by slug)",
    response_model=ResourceField,
    response_model_exclude_unset=True,
    tags=["Resource fields"],
)
@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/{{field_type}}/{{field_id}}",
    status_code=200,
    name="Get Resource field (by id)",
    response_model=ResourceField,
    response_model_exclude_unset=True,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_resource_field(
    request: Request,
    kbid: str,
    field_type: FieldTypeName,
    field_id: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
    show: List[ResourceFieldProperties] = Query([ResourceFieldProperties.VALUE]),
    extracted: List[ExtractedDataTypeName] = Query(
        [
            ExtractedDataTypeName.TEXT,
            ExtractedDataTypeName.METADATA,
            ExtractedDataTypeName.LINK,
            ExtractedDataTypeName.FILE,
        ]
    ),
    page: Union[Literal["last", "first"], int] = Query("last"),
) -> Response:
    storage = await get_storage(service_name=SERVICE_NAME)
    cache = await get_cache()
    driver = await get_driver()

    txn = await driver.begin()

    pb_field_id = FIELD_NAMES_TO_PB_TYPE_MAP[field_type]

    kb = ORMKnowledgeBox(txn, storage, cache, kbid)

    if rid is None:
        rid = await get_resource_uuid_by_slug(kbid, rslug, service_name=SERVICE_NAME)  # type: ignore
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
            resource_field.value = models.FieldText.from_message(value)

        if isinstance(value, resources_pb2.FieldFile):
            value = await field.get_value()
            resource_field.value = models.FieldFile.from_message(value)

        if isinstance(value, resources_pb2.FieldLink):
            value = await field.get_value()
            resource_field.value = models.FieldLink.from_message(value)

        if isinstance(value, resources_pb2.FieldLayout):
            value = await field.get_value()
            resource_field.value = models.FieldLayout.from_message(value)

        if isinstance(value, resources_pb2.FieldDatetime):
            value = await field.get_value()
            resource_field.value = models.FieldDatetime.from_message(value)

        if isinstance(value, resources_pb2.FieldKeywordset):
            value = await field.get_value()
            resource_field.value = models.FieldKeywordset.from_message(value)

        if isinstance(field, Conversation):
            if page == "first":
                page = 1
            elif page == "last":
                conversation_metadata = await field.get_metadata()
                page = conversation_metadata.pages

            value = await field.get_value(page=page)
            resource_field.value = models.Conversation.from_message(value)

    if ResourceFieldProperties.EXTRACTED in show and extracted:
        resource_field.extracted = FIELD_NAME_TO_EXTRACTED_DATA_FIELD_MAP[field_type]()
        await set_resource_field_extracted_data(
            field,
            resource_field.extracted,
            field_type,
            extracted,
        )

    if ResourceFieldProperties.ERROR in show:
        error = await field.get_error()
        if error is not None:
            resource_field.error = Error(body=error.error, code=error.code)

    await txn.abort()
    return Response(
        content=resource_field.json(exclude_unset=True), media_type="application/json"
    )
