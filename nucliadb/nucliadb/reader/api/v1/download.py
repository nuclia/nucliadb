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
from enum import Enum
from typing import Optional

from fastapi import HTTPException
from fastapi.requests import Request
from fastapi.responses import Response
from fastapi_versioning import version
from starlette.datastructures import Headers
from starlette.responses import StreamingResponse

from nucliadb.ingest.orm.resource import KB_REVERSE_REVERSE
from nucliadb.ingest.serialize import get_resource_uuid_by_slug
from nucliadb.models.common import FieldTypeName
from nucliadb.models.resource import NucliaDBRoles
from nucliadb.reader import SERVICE_NAME, logger
from nucliadb.reader.api.models import FIELD_NAMES_TO_PB_TYPE_MAP
from nucliadb_utils.authentication import requires_one
from nucliadb_utils.storages.storage import StorageField  # type: ignore
from nucliadb_utils.utilities import get_storage

from .router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api


class DownloadType(Enum):
    EXTRACTED = "extracted"
    FIELD = "field"


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/{{field_type}}/{{field_id}}/download/extracted/{{download_field:path}}",  # noqa
    tags=["Resource fields"],
    status_code=200,
    name="Download extracted binary file (by slug)",
)
@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/{{field_type}}/{{field_id}}/download/extracted/{{download_field:path}}",  # noqa
    tags=["Resource fields"],
    status_code=200,
    name="Download extracted binary file (by id)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_extract_file(
    request: Request,
    kbid: str,
    field_type: FieldTypeName,
    field_id: str,
    download_field: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> Response:

    rid = await _get_resource_uuid_from_params(kbid, rid, rslug)

    storage = await get_storage(service_name=SERVICE_NAME)

    pb_field_type = FIELD_NAMES_TO_PB_TYPE_MAP[field_type]
    field_type_letter = KB_REVERSE_REVERSE[pb_field_type]

    sf = storage.file_extracted(kbid, rid, field_type_letter, field_id, download_field)

    return await download_api(sf, request.headers)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field_id}}/download/field",
    tags=["Resource fields"],
    status_code=200,
    name="Download field binary field (by slug)",
)
@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field_id}}/download/field",
    tags=["Resource fields"],
    status_code=200,
    name="Download field binary field (by id)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_field_file(
    request: Request,
    kbid: str,
    field_id: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> Response:

    rid = await _get_resource_uuid_from_params(kbid, rid, rslug)

    storage = await get_storage(service_name=SERVICE_NAME)

    sf = storage.file_field(kbid, rid, field_id)

    return await download_api(sf, request.headers)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/layout/{{field_id}}/download/field/{{download_field}}",
    tags=["Resource fields"],
    status_code=200,
    name="Download layout binary field (by slug)",
)
@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/layout/{{field_id}}/download/field/{{download_field}}",
    tags=["Resource fields"],
    status_code=200,
    name="Download layout binary field (by id)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_field_layout(
    request: Request,
    kbid: str,
    field_id: str,
    download_field: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> Response:

    rid = await _get_resource_uuid_from_params(kbid, rid, rslug)

    storage = await get_storage(service_name=SERVICE_NAME)

    sf = storage.layout_field(kbid, rid, field_id, download_field)

    return await download_api(sf, request.headers)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/conversation/{{field_id}}/download/field/{{message_id}}/{{file_num}}",  # noqa
    tags=["Resource fields"],
    status_code=200,
    name="Download conversation binary field (by slug)",
)
@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/conversation/{{field_id}}/download/field/{{message_id}}/{{file_num}}",  # noqa
    tags=["Resource fields"],
    status_code=200,
    name="Download conversation binary field (by id)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_field_conversation(
    request: Request,
    kbid: str,
    field_id: str,
    message_id: str,
    file_num: int,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> Response:
    rid = await _get_resource_uuid_from_params(kbid, rid, rslug)

    storage = await get_storage(service_name=SERVICE_NAME)

    sf = storage.conversation_field(kbid, rid, field_id, message_id, file_num)

    return await download_api(sf, request.headers)


async def download_api(sf: StorageField, headers: Headers):

    metadata = await sf.exists()
    if metadata is None:
        raise HTTPException(status_code=404, detail="Specified file doesn't exist")

    file_size = int(metadata.get("SIZE", -1))
    content_type = metadata.get("CONTENT_TYPE", "application/octet-stream")
    filename = metadata.get("FILENAME", "file")
    status_code = 200

    extra_headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": content_type,
        "Content-Disposition": '{}; filename="{}"'.format("attachment", filename),
    }
    download_headers = {}

    if "range" in headers and file_size > -1:
        range_request = headers["range"]
        try:
            start_str, _, end_str = range_request.split("bytes=")[-1].partition("-")
            start = int(start_str)
            if len(end_str) == 0:
                # bytes=0- is valid
                end = file_size - 1
            end = int(end_str) + 1  # python is inclusive, http is exclusive
        except (IndexError, ValueError):
            # range errors fallback to full download
            raise HTTPException(
                detail={"reason": "rangeNotParsable", "range": range_request},
                headers={"Content-Range": f"bytes */{file_size}"},
                status_code=416,
            )
        if start > end or start < 0:
            raise HTTPException(
                detail={
                    "reason": "invalidRange",
                    "range": range_request,
                    "message": "Invalid range",
                },
                headers={"Content-Range": f"bytes */{file_size}"},
                status_code=416,
            )
        if end > file_size:
            raise HTTPException(
                detail={
                    "reason": "invalidRange",
                    "range": range_request,
                    "message": "Invalid range, too large end value",
                },
                headers={"Content-Range": f"bytes */{file_size}"},
                status_code=416,
            )
        status_code = 206
        logger.debug(f"Range request: {range_request}")
        extra_headers["Content-Length"] = f"{end - start}"
        extra_headers["Content-Range"] = f"bytes {start}-{end - 1}/{file_size}"
        download_headers["Range"] = range_request

    return StreamingResponse(
        sf.storage.download(sf.bucket, sf.key, headers=download_headers),  # type: ignore
        status_code=status_code,
        media_type=content_type,
        headers=extra_headers,
    )


async def _get_resource_uuid_from_params(
    kbid, rid: Optional[str], rslug: Optional[str]
) -> str:
    if not any([rid, rslug]):
        raise ValueError("Either rid or slug must be set")

    if not rid:
        # Attempt to get it from slug
        rid = await get_resource_uuid_by_slug(kbid, rslug, service_name=SERVICE_NAME)  # type: ignore
        if rid is None:
            raise HTTPException(status_code=404, detail="Resource does not exist")

    return rid
