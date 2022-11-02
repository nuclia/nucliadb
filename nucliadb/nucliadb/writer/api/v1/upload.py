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
import base64
import pickle
import uuid
from datetime import datetime
from hashlib import md5
from io import BytesIO
from typing import List, Optional

from fastapi import HTTPException
from fastapi.params import Header
from fastapi.requests import Request
from fastapi.responses import Response
from fastapi_versioning import version  # type: ignore
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.resources_pb2 import FieldFile
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    ResourceFieldExistsResponse,
    ResourceFieldId,
)
from starlette.requests import Request as StarletteRequest

from nucliadb.ingest.orm.utils import set_title
from nucliadb.ingest.processing import PushPayload, Source
from nucliadb.models.resource import NucliaDBRoles
from nucliadb.models.writer import CreateResourcePayload, ResourceFileUploaded
from nucliadb.writer import SERVICE_NAME
from nucliadb.writer.api.v1.resource import get_rid_from_params_or_raise_error
from nucliadb.writer.exceptions import (
    ConflictError,
    IngestNotAvailable,
    ResourceNotFound,
)
from nucliadb.writer.resource.audit import parse_audit
from nucliadb.writer.resource.basic import parse_basic, set_last_seqid
from nucliadb.writer.resource.field import parse_fields
from nucliadb.writer.resource.origin import parse_origin
from nucliadb.writer.tus import TUSUPLOAD, UPLOAD, get_dm, get_storage_manager
from nucliadb.writer.tus.exceptions import (
    HTTPBadRequest,
    HTTPConflict,
    HTTPNotFound,
    HTTPPreconditionFailed,
    HTTPServiceUnavailable,
    InvalidTUSMetadata,
)
from nucliadb.writer.tus.utils import parse_tus_metadata
from nucliadb.writer.utilities import get_processing
from nucliadb_telemetry.utils import set_info_on_span
from nucliadb_utils.authentication import requires_one
from nucliadb_utils.exceptions import LimitsExceededError
from nucliadb_utils.storages.storage import KB_RESOURCE_FIELD
from nucliadb_utils.utilities import (
    get_ingest,
    get_partitioning,
    get_storage,
    get_transaction,
)

from .router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api

TUS_HEADERS = {
    "Tus-Resumable": "1.0.0",
    "Tus-Version": "1.0.0",
    "Tus-Extension": "creation-defer-length",
}


@api.options(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field}}/{TUSUPLOAD}/{{upload_id}}",
    include_in_schema=False,
)
@api.options(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field}}/{TUSUPLOAD}/{{upload_id}}",
    include_in_schema=False,
)
@api.options(
    f"/{KB_PREFIX}/{{kbid}}/{TUSUPLOAD}/{{upload_id}}",
    include_in_schema=False,
)
@api.options(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field}}/{TUSUPLOAD}",
    tags=["Resource field TUS uploads"],
    name="TUS Server information",
    openapi_extra={"x-operation-order": 4},
    include_in_schema=False,
)
@api.options(
    f"/{KB_PREFIX}/{{kbid}}/{TUSUPLOAD}",
    tags=["Knowledge Box TUS uploads"],
    name="TUS Server information",
    openapi_extra={"x-operation-order": 4},
)
@version(1)
async def options_single(
    request: Request,
    kbid: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
    upload_id: Optional[str] = None,
    field: Optional[str] = None,
) -> Response:
    """
    Gather information about the Server’s current configuration such as enabled extensions, version...
    """
    resp = Response(headers=TUS_HEADERS, status_code=204)
    return resp


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field}}/{TUSUPLOAD}",
    tags=["Resource field TUS uploads"],
    name="Create new upload on a Resource (by slug)",
    openapi_extra={"x-operation-order": 1},
)
@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{path_rid}}/file/{{field}}/{TUSUPLOAD}",
    tags=["Resource field TUS uploads"],
    name="Create new upload on a Resource (by id)",
    openapi_extra={"x-operation-order": 1},
)
@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{TUSUPLOAD}",
    tags=["Knowledge Box TUS uploads"],
    name="Create new upload on a Knowledge Box",
    openapi_extra={"x-operation-order": 1},
)
@requires_one([NucliaDBRoles.WRITER])
@version(1)
async def post(
    request: Request,
    kbid: str,
    item: Optional[CreateResourcePayload] = None,
    path_rid: Optional[str] = None,
    rslug: Optional[str] = None,
    field: Optional[str] = None,
) -> Response:
    """
    An empty POST request is used to create a new upload resource.
    The Upload-Length header indicates the size of the entire upload in bytes.
    """
    dm = get_dm()
    storage_manager = get_storage_manager()

    if rslug:
        path_rid = await get_rid_from_params_or_raise_error(kbid, slug=rslug)

    implies_resource_creation = path_rid is None

    deferred_length = False
    if request.headers.get("upload-defer-length") == "1":
        deferred_length = True

    size = None
    if "upload-length" in request.headers:
        size = int(request.headers["upload-length"])
    else:
        if not deferred_length:
            raise HTTPPreconditionFailed(detail="We need upload-length header")

    if "tus-resumable" not in request.headers:
        raise HTTPPreconditionFailed(detail="TUS needs a TUS version")

    if "upload-metadata" in request.headers:
        try:
            metadata = parse_tus_metadata(request.headers["upload-metadata"])
        except InvalidTUSMetadata as exc:
            raise HTTPBadRequest(
                detail=f"Upload-Metadata header contains errors: {str(exc)}"
            )
    else:
        metadata = {}

    try:
        path, rid, field = await start_upload_field(
            kbid, path_rid, field, metadata.get("md5")
        )
    except ResourceNotFound:
        raise HTTPNotFound("Resource is not found or not yet available")
    except ConflictError:
        raise HTTPConflict("A resource with the same uploaded file already exists")
    except IngestNotAvailable:
        raise HTTPServiceUnavailable("Upload not available right now, try again")

    if implies_resource_creation:
        # When uploading a file to a new kb resource, we want to  allow multiple
        # concurrent uploads, so upload id will be randmon
        upload_id = uuid.uuid4().hex
    else:
        # When uploading to a specific resourece field, we want make sure that only one
        # upload is active at a time, so by default, unless you explicitly override it with the header,
        # concurrent uploads to the same file field will be blocked
        upload_id = md5(f"{kbid}__{rid}__{field}".encode()).hexdigest()

    # This only happens in tus-java-client, redirect this POST to a PATCH
    if request.headers.get("x-http-method-override") == "PATCH":
        return await patch(request, upload_id)

    # We need a content_type value set
    # - content-type set by the user in tus-metadata will take precedence
    # - content-type of the request, only when uploading a field into a resource
    # - otherwise, we'll set it to a generic binary content type
    # We don't use the request content type, as for kb uploads it will be json,
    # containing a resource definition
    request_content_type = (
        request.headers.get("content-type", "application/octet-stream")
        if item is None
        else "application/octet-stream"
    )
    metadata.setdefault("content_type", request_content_type)

    if "filename" not in metadata:
        metadata["filename"] = uuid.uuid4().hex

    metadata["implies_resource_creation"] = implies_resource_creation

    creation_payload = None
    if implies_resource_creation:
        creation_payload = base64.b64encode(pickle.dumps(item)).decode()

    await dm.load(upload_id)
    await dm.start(request)
    await dm.update(
        upload_file_id=f"{upload_id}",
        rid=rid,
        field=field,
        metadata=metadata,
        deferred_length=deferred_length,
        offset=0,
        item=creation_payload,
    )

    if size is not None:
        await dm.update(
            size=size,
        )

    await storage_manager.start(dm, path=path, kbid=kbid)
    await dm.save()

    location = f"{request['path']}/{upload_id}"
    return Response(
        status_code=201,
        headers={
            "Location": location,  # noqa
            "Tus-Resumable": "1.0.0",
            "Access-Control-Expose-Headers": "Location,Tus-Resumable",
        },
    )


@api.head(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field}}/{TUSUPLOAD}/{{upload_id}}",
    tags=["Resource field TUS uploads"],
    status_code=200,
    openapi_extra={"x-operation-order": 3},
    name="Upload information",
)
@api.head(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field}}/{TUSUPLOAD}/{{upload_id}}",
    tags=["Resource field TUS uploads"],
    status_code=200,
    openapi_extra={"x-operation-order": 3},
    name="Upload information",
)
@api.head(
    f"/{KB_PREFIX}/{{kbid}}/{TUSUPLOAD}/{{upload_id}}",
    tags=["Knowledge Box TUS uploads"],
    status_code=200,
    openapi_extra={"x-operation-order": 3},
    name="Upload information",
)
@requires_one([NucliaDBRoles.WRITER])
@version(1)
async def head(
    request: Request,
    upload_id: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
    field: Optional[str] = None,
) -> Response:
    """
    Get information about a current download (completed upload size)
    """
    dm = get_dm()
    await dm.load(upload_id)
    head_response = {
        "Upload-Offset": str(dm.offset),
        "Tus-Resumable": "1.0.0",
        "Access-Control-Expose-Headers": "Upload-Offset,Tus-Resumable,Upload-Length",
    }
    if dm.get("size"):
        head_response["Upload-Length"] = str(dm.get("size"))
    else:
        head_response["Upload-Length"] = "0"
    return Response(headers=head_response)


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field}}/{TUSUPLOAD}/{{upload_id}}",
    tags=["Resource field TUS uploads"],
    status_code=200,
    name="Upload data on a Resource (by slug)",
    openapi_extra={"x-operation-order": 2},
)
@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field}}/{TUSUPLOAD}/{{upload_id}}",
    tags=["Resource field TUS uploads"],
    status_code=200,
    name="Upload data on a Resource (by id)",
    openapi_extra={"x-operation-order": 2},
)
@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/{TUSUPLOAD}/{{upload_id}}",
    tags=["Knowledge Box TUS uploads"],
    status_code=200,
    name="Upload data on a Knowledge Box",
    openapi_extra={"x-operation-order": 2},
)
@requires_one([NucliaDBRoles.WRITER])
@version(1)
async def patch(
    request: Request,
    kbid: str,
    upload_id: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
    field: Optional[str] = None,
    x_synchronous: bool = Header(False),  # type: ignore
) -> Response:
    """
    Upload all bytes in the requests and append them in the specifyied offset
    """
    if rslug:
        rid = await get_rid_from_params_or_raise_error(kbid, slug=rslug)

    dm = get_dm()
    await dm.load(upload_id)
    to_upload = None
    if "content-length" in request.headers:
        # header is optional, we'll be okay with unknown lengths...
        to_upload = int(request.headers["content-length"])

    if "upload-length" in request.headers:
        if dm.get("deferred_length"):
            size = int(request.headers["upload-length"])
            await dm.update(size=size)

    if "upload-offset" in request.headers:
        offset = int(request.headers["upload-offset"])
    else:
        raise HTTPPreconditionFailed(detail="No upload-offset header")

    if offset != dm.offset:
        raise HTTPConflict(
            detail=f"Current upload offset({offset}) does not match "
            f"object offset {dm.offset}"
        )

    storage_manager = get_storage_manager()
    read_bytes = await storage_manager.append(
        dm,
        storage_manager.iterate_body_chunks(request, storage_manager.chunk_size),
        offset,
    )

    if to_upload and read_bytes != to_upload:  # pragma: no cover
        # check length matches if provided
        raise HTTPPreconditionFailed(
            detail="Upload size does not match what was provided"
        )
    await dm.update(offset=offset + read_bytes)

    headers = {
        "Upload-Offset": str(dm.offset),
        "Tus-Resumable": "1.0.0",
        "Access-Control-Expose-Headers": ",".join(
            ["Upload-Offset", "Tus-Resumable", "Tus-Upload-Finished"]
        ),
    }

    upload_finished = dm.get("size") is not None and dm.offset >= dm.get("size")

    if upload_finished:
        rid = dm.get("rid", rid)
        if rid is None:
            raise AttributeError()
        field = dm.get("field", field)
        if field is None:
            raise AttributeError()
        path = await storage_manager.finish(dm)
        headers["Tus-Upload-Finished"] = "1"
        headers["NDB-Resource"] = f"/{KB_PREFIX}/{kbid}/resources/{rid}"
        headers["NDB-Field"] = f"/{KB_PREFIX}/{kbid}/resources/{rid}/field/{field}"

        item_payload = dm.get("item")
        creation_payload = None
        if item_payload is not None:
            if isinstance(item_payload, str):
                item_payload = item_payload.encode()
            creation_payload = pickle.loads(base64.b64decode(item_payload))
        try:
            seqid = await store_file_on_nuclia_db(
                size=dm.get("size"),
                content_type=dm.get("metadata", {}).get("content_type"),
                override_resource_title=dm.get("metadata", {}).get(
                    "implies_resource_creation", False
                ),
                filename=dm.get("metadata", {}).get("filename"),
                password=dm.get("metadata", {}).get("password"),
                language=dm.get("metadata", {}).get("language"),
                md5=dm.get("metadata", {}).get("md5"),
                source=storage_manager.storage.source,
                field=field,
                rid=rid,
                kbid=kbid,
                path=path,
                request=request,
                bucket=await storage_manager.storage.get_bucket_name(kbid),
                item=creation_payload,
                wait_on_commit=x_synchronous,
            )
        except LimitsExceededError as exc:
            raise HTTPException(status_code=402, detail=str(exc))

        headers["NDB-Seq"] = f"{seqid}"
    else:
        await dm.save()

    return Response(headers=headers)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field}}/{UPLOAD}",
    status_code=201,
    tags=["Resource fields"],
    name="Upload binary file on a Resource (by slug)",
    description="Upload a file as a field on an existing resource, if the field exists will return a conflict (419)",
)
@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{path_rid}}/file/{{field}}/{UPLOAD}",
    status_code=201,
    tags=["Resource fields"],
    name="Upload binary file on a Resource (by id)",
    description="Upload a file as a field on an existing resource, if the field exists will return a conflict (419)",
)
@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{UPLOAD}",
    status_code=201,
    tags=["Knowledge Boxes"],
    name="Upload binary file on a Knowledge Box",
    description="Upload a file onto a Knowledge Box, field id will be file and rid will be autogenerated. ",
)
@requires_one([NucliaDBRoles.WRITER])
@version(1)
async def upload(
    request: StarletteRequest,
    kbid: str,
    path_rid: Optional[str] = None,
    rslug: Optional[str] = None,
    field: Optional[str] = None,
    x_filename: Optional[List[str]] = Header(None),  # type: ignore
    x_password: Optional[List[str]] = Header(None),  # type: ignore
    x_language: Optional[List[str]] = Header(None),  # type: ignore
    x_md5: Optional[List[str]] = Header(None),  # type: ignore
    x_synchronous: bool = Header(False),  # type: ignore
) -> ResourceFileUploaded:

    if rslug:
        path_rid = await get_rid_from_params_or_raise_error(kbid, slug=rslug)

    md5_user = x_md5[0] if x_md5 is not None and len(x_md5) > 0 else None
    try:
        path, rid, valid_field = await start_upload_field(
            kbid, path_rid, field, md5_user
        )
    except ResourceNotFound:
        raise HTTPNotFound("Resource is not found or not yet available")
    except ConflictError:
        raise HTTPConflict("A resource with the same uploaded file already exists")
    except IngestNotAvailable:
        raise HTTPServiceUnavailable("Upload not available right now, try again")

    dm = get_dm()
    storage_manager = get_storage_manager()

    implies_resource_creation = path_rid is None

    if implies_resource_creation:
        # When uploading a file to a new kb resource, we want to  allow multiple
        # concurrent uploads, so upload id will be randmon
        upload_id = uuid.uuid4().hex
    else:
        # When uploading to a specific resourece field, we want make sure that only one
        # upload is active at a time, so by default, unless you explicitly override it with the header,
        # concurrent uploads to the same file field will be blocked
        upload_id = md5(f"{kbid}__{rid}__{field}".encode()).hexdigest()

    await dm.load(upload_id)

    await dm.start(request)

    # We need a content_type value set
    # - content-type set by the user in the upload request header
    # - if not set, we'll set it to a generic binary content type
    content_type = request.headers.get("content-type", "application/octet-stream")

    if x_filename and len(x_filename):
        filename = maybe_b64decode(x_filename[0])
    else:
        filename = uuid.uuid4().hex

    metadata = {"content_type": content_type, "filename": filename}

    await dm.update(
        upload_file_id=f"{upload_id}",
        size=request.headers.get("content-length", None),
        metadata=metadata,
        offset=0,
    )

    await storage_manager.start(dm, path=path, kbid=kbid)

    async def generate_buffer(storage_manager, request):
        buf = BytesIO()
        async for chunk in request.stream():
            buf.write(chunk)
            while buf.tell() > storage_manager.chunk_size:
                buf.seek(0)
                data = buf.read(storage_manager.chunk_size)
                if len(data):
                    yield data
                old_data = buf.read()
                buf = BytesIO()
                buf.write(old_data)
        buf.seek(0)
        data = buf.read()
        if len(data):
            yield data

    size = await storage_manager.append(
        dm, generate_buffer(storage_manager=storage_manager, request=request), 0
    )
    await storage_manager.finish(dm)
    try:
        seqid = await store_file_on_nuclia_db(
            size=size,
            kbid=kbid,
            content_type=content_type,
            override_resource_title=implies_resource_creation,
            filename=filename,
            password=x_password[0] if x_password and len(x_password) else None,
            language=x_language[0] if x_language and len(x_language) else None,
            md5=x_md5[0] if x_md5 and len(x_md5) else None,
            field=valid_field,
            source=storage_manager.storage.source,
            rid=rid,
            path=path,
            request=request,
            bucket=await storage_manager.storage.get_bucket_name(kbid),
            wait_on_commit=x_synchronous,
        )
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    return ResourceFileUploaded(seqid=seqid, uuid=rid, field_id=valid_field)


async def start_upload_field(
    kbid: str,
    rid: Optional[str] = None,
    field: Optional[str] = None,
    md5: Optional[str] = None,
):
    ingest = get_ingest()
    pbrequest = ResourceFieldId()
    pbrequest.kbid = kbid
    if rid is not None:
        pbrequest.rid = rid

    elif rid is None and md5 is not None:
        pbrequest.rid = md5

    try:
        response: ResourceFieldExistsResponse = await ingest.ResourceFieldExists(pbrequest)  # type: ignore
    except AioRpcError as exc:
        if exc.code() is GrpcStatusCode.UNAVAILABLE:
            raise IngestNotAvailable()
        else:
            raise exc

    if response.found is False and rid is not None:
        raise ResourceNotFound()
    elif response.found is True and rid is None and md5 is not None:
        raise ConflictError()

    if rid is None and md5 is None:
        rid = uuid.uuid4().hex
    elif rid is None:
        rid = md5

    if field is None and md5 is None:
        field = uuid.uuid4().hex
    elif field is None:
        field = md5

    if rid and kbid and field:
        set_info_on_span(
            {"nuclia.rid": rid, "nuclia.kbid": kbid, "nuclia.field": field}
        )

    path = KB_RESOURCE_FIELD.format(kbid=kbid, uuid=rid, field=field)
    return path, rid, field


async def store_file_on_nuclia_db(
    size: int,
    kbid: str,
    path: str,
    request: Request,
    bucket: str,
    source: Source,
    rid: str,
    field: str,
    content_type: str = "application/octet-stream",
    override_resource_title: bool = False,
    filename: Optional[str] = None,
    password: Optional[str] = None,
    language: Optional[str] = None,
    md5: Optional[str] = None,
    item: Optional[CreateResourcePayload] = None,
    wait_on_commit: bool = False,
) -> int:
    # File is on NucliaDB Storage at path

    partitioning = get_partitioning()
    transaction = get_transaction()
    processing = get_processing()
    storage = await get_storage(service_name=SERVICE_NAME)

    partition = partitioning.generate_partition(kbid, rid)

    writer = BrokerMessage()
    toprocess = PushPayload(
        uuid=rid,
        kbid=kbid,
        partition=partition,
        userid=request.headers.get("X-NUCLIADB-USER", ""),
    )

    writer.kbid = kbid
    toprocess.kbid = kbid
    writer.uuid = rid
    toprocess.uuid = rid
    toprocess.source = Source.HTTP

    parse_audit(writer.audit, request)

    if item is not None:
        if item.slug:
            writer.slug = item.slug
            toprocess.slug = item.slug

        toprocess.processing_options = item.processing_options

        parse_basic(writer, item, toprocess)
        if item.origin is not None:
            parse_origin(writer.origin, item.origin)

        await parse_fields(
            writer=writer,
            item=item,
            toprocess=toprocess,
            kbid=kbid,
            uuid=rid,
            x_skip_store=False,
        )

    if override_resource_title and filename is not None:
        set_title(writer, toprocess, filename)
    writer.basic.icon = content_type
    writer.basic.created.FromDatetime(datetime.now())

    # Update resource with file
    file_field = FieldFile()
    file_field.added.FromDatetime(datetime.now())
    file_field.file.bucket_name = bucket
    file_field.file.content_type = content_type
    if filename is not None:
        file_field.file.filename = filename
    file_field.file.uri = path
    file_field.file.source = source

    if md5:
        file_field.file.md5 = md5
    if size:
        file_field.file.size = size
    if language:
        file_field.language = language
    if password:
        file_field.password = password

    writer.files[field].CopyFrom(file_field)
    toprocess.filefield[field] = await processing.convert_internal_filefield_to_str(
        file_field, storage=storage
    )

    try:
        seqid = await processing.send_to_process(toprocess, partition)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    writer.source = BrokerMessage.MessageSource.WRITER
    set_last_seqid(writer, seqid)
    await transaction.commit(writer, partition, wait=wait_on_commit)

    return seqid


def maybe_b64decode(some_string: str) -> str:
    try:
        return base64.b64decode(some_string).decode()
    except ValueError:
        # not b64encoded
        return some_string
