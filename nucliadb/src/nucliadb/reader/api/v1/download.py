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
import urllib.parse
from typing import Annotated

from fastapi import HTTPException
from fastapi.requests import Request
from fastapi.responses import Response
from fastapi_versioning import version
from starlette.responses import StreamingResponse

from nucliadb.common import datamanagers
from nucliadb.common.ids import FIELD_TYPE_PB_TO_STR
from nucliadb.common.models_utils import to_proto
from nucliadb.reader import RANGE_HEADER, SERVICE_NAME, logger
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires_one
from nucliadb_utils.storages.storage import ObjectMetadata, Range, StorageField
from nucliadb_utils.utilities import get_storage

from .router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/{{field_type}}/{{field_id}}/download/extracted/{{download_field:path}}",
    tags=["Resource fields"],
    status_code=200,
    summary="Download extracted binary file (by slug)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_extract_file_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_type: FieldTypeName,
    field_id: str,
    download_field: str,
    range: Annotated[str | None, RANGE_HEADER] = None,
) -> Response:
    return await _download_extract_file(
        kbid,
        field_type,
        field_id,
        download_field,
        rslug=rslug,
        range_request=range,
    )


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/{{field_type}}/{{field_id}}/download/extracted/{{download_field:path}}",
    tags=["Resource fields"],
    status_code=200,
    summary="Download extracted binary file (by id)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_extract_file_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_type: FieldTypeName,
    field_id: str,
    download_field: str,
    range: Annotated[str | None, RANGE_HEADER] = None,
) -> Response:
    return await _download_extract_file(
        kbid, field_type, field_id, download_field, rid=rid, range_request=range
    )


async def _download_extract_file(
    kbid: str,
    field_type: FieldTypeName,
    field_id: str,
    download_field: str,
    rid: str | None = None,
    rslug: str | None = None,
    range_request: str | None = None,
) -> Response:
    rid = await _get_resource_uuid_from_params(kbid, rid, rslug)

    storage = await get_storage(service_name=SERVICE_NAME)

    pb_field_type = to_proto.field_type_name(field_type)
    field_type_letter = FIELD_TYPE_PB_TO_STR[pb_field_type]

    sf = storage.file_extracted(kbid, rid, field_type_letter, field_id, download_field)

    return await download_api(sf, range_request)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field_id}}/download/field",
    tags=["Resource fields"],
    status_code=200,
    summary="Download field binary field (by slug)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_field_file_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    inline: bool = False,
    range: Annotated[str | None, RANGE_HEADER] = None,
) -> Response:
    return await _download_field_file(kbid, field_id, rslug=rslug, range_request=range, inline=inline)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field_id}}/download/field",
    tags=["Resource fields"],
    status_code=200,
    summary="Download field binary field (by id)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_field_file_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    inline: bool = False,
    range: Annotated[str | None, RANGE_HEADER] = None,
) -> Response:
    return await _download_field_file(kbid, field_id, rid=rid, range_request=range, inline=inline)


async def _download_field_file(
    kbid: str,
    field_id: str,
    rid: str | None = None,
    rslug: str | None = None,
    range_request: str | None = None,
    inline: bool = False,
) -> Response:
    rid = await _get_resource_uuid_from_params(kbid, rid, rslug)

    storage = await get_storage(service_name=SERVICE_NAME)

    sf = storage.file_field(kbid, rid, field_id)

    return await download_api(sf, range_request=range_request, inline=inline)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/conversation/{{field_id}}/download/field/{{message_id}}/{{file_num}}",
    tags=["Resource fields"],
    status_code=200,
    summary="Download conversation binary field (by slug)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_field_conversation_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    message_id: str,
    file_num: int,
    range: Annotated[str | None, RANGE_HEADER] = None,
) -> Response:
    return await _download_field_conversation_attachment(
        kbid,
        field_id,
        message_id,
        file_num,
        rslug=rslug,
        range_request=range,
    )


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/conversation/{{field_id}}/download/field/{{message_id}}/{{file_num}}",
    tags=["Resource fields"],
    status_code=200,
    summary="Download conversation binary field (by id)",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def download_field_conversation_attachment_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    message_id: str,
    file_num: int,
    range: Annotated[str | None, RANGE_HEADER] = None,
) -> Response:
    return await _download_field_conversation_attachment(
        kbid,
        field_id,
        message_id,
        file_num,
        rid=rid,
        range_request=range,
    )


async def _download_field_conversation_attachment(
    kbid: str,
    field_id: str,
    message_id: str,
    file_num: int,
    rid: str | None = None,
    rslug: str | None = None,
    range_request: str | None = None,
) -> Response:
    rid = await _get_resource_uuid_from_params(kbid, rid, rslug)

    storage = await get_storage(service_name=SERVICE_NAME)

    sf = storage.conversation_field_attachment(
        kbid, rid, field_id, message_id, attachment_index=file_num
    )

    return await download_api(sf, range_request)


async def download_api(sf: StorageField, range_request: str | None = None, inline: bool = False):
    metadata: ObjectMetadata | None = await sf.exists()
    if metadata is None:
        raise HTTPException(status_code=404, detail="Specified file doesn't exist")

    file_size = metadata.size or -1
    content_type = metadata.content_type or "application/octet-stream"
    filename = metadata.filename or "file"
    filename = safe_http_header_encode(filename)

    status_code = 200

    content_disposition = "inline" if inline else f'attachment; filename="{filename}"'
    extra_headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": content_type,
        "Content-Disposition": content_disposition,
    }

    range = Range()
    if range_request and file_size > -1:
        status_code = 206
        try:
            start, end, range_size = parse_media_range(range_request, file_size)
        except NotImplementedError:
            raise HTTPException(
                detail={
                    "reason": "rangeNotSupported",
                    "range": range_request,
                    "message": "Multipart ranges are not supported yet",
                },
                headers={"Content-Range": f"bytes */{file_size}"},
                status_code=416,
            )
        except (IndexError, ValueError):
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
        logger.debug(f"Range request: {range_request}")
        extra_headers["Content-Length"] = f"{range_size}"
        extra_headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"
        range.start = start
        range.end = end

    return StreamingResponse(
        sf.storage.download(sf.bucket, sf.key, range=range),
        status_code=status_code,
        media_type=content_type,
        headers=extra_headers,
    )


async def _get_resource_uuid_from_params(kbid, rid: str | None, rslug: str | None) -> str:
    if not any([rid, rslug]):
        raise ValueError("Either rid or slug must be set")

    if not rid:
        # Attempt to get it from slug
        rid = await datamanagers.atomic.resources.get_resource_uuid_from_slug(
            kbid=kbid,
            # mypy doesn't infer that we already checked for slug to be something
            slug=rslug,  # type: ignore[arg-type]
        )
        if rid is None:
            raise HTTPException(status_code=404, detail="Resource does not exist")

    return rid


def parse_media_range(range_request: str, file_size: int) -> tuple[int, int, int]:
    # Implemented following this docpage: https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests
    ranges = range_request.split("bytes=")[-1].split(", ")
    if len(ranges) > 1:
        raise NotImplementedError()
    start_str, _, end_str = ranges[0].partition("-")
    start = int(start_str)
    max_range_size = file_size - 1
    if len(end_str) == 0:
        # bytes=0- is valid
        end = max_range_size
        range_size = file_size - start
    else:
        end = int(end_str)
        end = min(end, max_range_size)
        range_size = (end - start) + 1
    return start, end, range_size


def safe_http_header_encode(text):
    return urllib.parse.quote(text)
