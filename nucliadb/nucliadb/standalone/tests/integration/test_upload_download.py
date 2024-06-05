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
from io import BytesIO

import pytest

from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCES_PREFIX
from nucliadb.writer.settings import settings as writer_settings
from nucliadb.writer.tests.test_files import ASSETS_PATH as WRITER_ASSETS_PATH
from nucliadb.writer.tus import TUSUPLOAD


@pytest.fixture(scope="function")
def configure_redis_dm(redis):
    writer_settings.dm_enabled = True
    writer_settings.dm_redis_host = redis[0]
    writer_settings.dm_redis_port = redis[1]
    yield


def header_encode(some_string):
    return base64.b64encode(some_string.encode()).decode()


@pytest.mark.asyncio
async def test_file_tus_upload_and_download(
    configure_redis_dm, nucliadb_writer, nucliadb_reader, knowledgebox_one
):
    language = "ca"
    filename = "image.jpg"
    md5 = "7af0916dba8b70e29d99e72941923529"
    content_type = "image/jpg"

    # Create a resource
    kb_path = f"/{KB_PREFIX}/{knowledgebox_one}"
    resp = await nucliadb_writer.post(
        f"{kb_path}/{RESOURCES_PREFIX}",
        json={
            "slug": "resource1",
            "title": "Resource 1",
        },
    )
    assert resp.status_code == 201
    resource = resp.json().get("uuid")

    # Start TUS upload
    url = f"{kb_path}/{RESOURCE_PREFIX}/{resource}/file/field1/{TUSUPLOAD}"
    upload_metadata = ",".join([
        f"filename {header_encode(filename)}",
        f"language {header_encode(language)}",
        f"md5 {header_encode(md5)}",
    ])
    resp = await nucliadb_writer.post(
        url,
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": upload_metadata,
            "content-type": content_type,
            "upload-defer-length": "1",
        },
    )
    assert resp.status_code == 201
    # Get the URL to upload the file to
    url = resp.headers["location"]

    # Read file in memory
    file_content = BytesIO()
    with open(f"{WRITER_ASSETS_PATH}/image001.jpg", "rb") as f:
        file_content.write(f.read())
    file_content.seek(0)

    # Upload the file part by part
    chunk_size = 10_000
    chunks_uploaded = 0
    offset = 0
    chunk = file_content.read(chunk_size)
    while chunk != b"":
        chunks_uploaded += 1

        # Make sure the upload is at the right offset
        resp = await nucliadb_writer.head(url)
        assert resp.headers["Upload-Length"] == f"0"
        assert resp.headers["Upload-Offset"] == f"{offset}"

        headers = {
            "upload-offset": f"{offset}",
            "content-length": f"{len(chunk)}",
        }
        if len(chunk) < chunk_size:
            # If this is the last part, we need to set the upload-length header
            headers["upload-length"] = f"{offset + len(chunk)}"

        # Upload the chunk
        resp = await nucliadb_writer.patch(
            url,
            data=chunk,
            headers=headers,
        )
        offset += len(chunk)
        chunk = file_content.read(chunk_size)

    # Check that we tested at least with 2 chunks
    assert chunks_uploaded >= 2

    # Make sure the upload is finished on the server side
    assert resp.headers["Tus-Upload-Finished"] == "1"

    # Now download the file
    download_url = f"{kb_path}/{RESOURCE_PREFIX}/{resource}/file/field1/download/field"
    resp = await nucliadb_reader.get(download_url)
    assert resp.status_code == 200
    # Make sure the filename and contents are correct
    assert resp.headers["Content-Disposition"] == f'attachment; filename="{filename}"'
    assert resp.headers["Content-Type"] == content_type
    assert resp.content == file_content.getvalue()

    breakpoint()
    # Download the file with range headers
    range_downloaded = BytesIO()
    download_url = f"{kb_path}/{RESOURCE_PREFIX}/{resource}/file/field1/download/field"
    # One chunk first
    resp = await nucliadb_reader.get(
        download_url,
        headers={
            "Range": "bytes=0-100",
        },
        timeout=None,
    )
    assert resp.status_code == 200
    range_downloaded.write(resp.content)
    breakpoint()

    # Some more
    resp = await nucliadb_reader.get(
        download_url,
        headers={
            "Range": "bytes=101-200",
        },
    )
    assert resp.status_code == 200
    range_downloaded.write(resp.content)
    breakpoint()

    # The rest of the file
    resp = await nucliadb_reader.get(
        download_url,
        headers={
            "Range": "bytes=201-",
        },
        timeout=None,
    )
    assert resp.status_code == 200
    range_downloaded.write(resp.content)
    breakpoint()

    # Make sure the downloaded content is the same as the original file
    file_content.seek(0)
    range_downloaded.seek(0)
    assert file_content.getvalue() == range_downloaded.getvalue()



@pytest.mark.asyncio
async def test_tus_upload_handles_unknown_upload_ids(
    configure_redis_dm, nucliadb_writer, nucliadb_reader, knowledgebox_one
):
    kbid = knowledgebox_one
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/{TUSUPLOAD}/foobarid",
        headers={},
        data=b"foobar",
    )
    assert resp.status_code == 404
    error_detail = resp.json().get("detail")
    assert error_detail == "Resumable URI not found for upload_id: foobarid"
