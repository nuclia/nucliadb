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
import sys
from io import BytesIO

import pytest
from httpx import AsyncClient
from pytest_lazy_fixtures import lazy_fixture

from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCES_PREFIX
from nucliadb.writer.settings import settings as writer_settings
from nucliadb.writer.tus import TUSUPLOAD, get_storage_manager
from nucliadb_models import content_types
from nucliadb_utils.storages.storage import Storage


@pytest.fixture(scope="function")
def configure_redis_dm(valkey):
    writer_settings.dm_enabled = True
    writer_settings.dm_redis_host = valkey[0]
    writer_settings.dm_redis_port = valkey[1]
    yield


def header_encode(some_string):
    return base64.b64encode(some_string.encode()).decode()


storages = [
    lazy_fixture.lf("s3_storage"),
    lazy_fixture.lf("local_storage"),
    # lazy_fixture.lf("azure_storage"),
]
if "darwin" not in sys.platform:
    # Nidx fails on MAC when using the gcs backend. To be looked into!
    storages.append(lazy_fixture.lf("gcs_storage"))

# TODO: Azure blob storage not supported by nidx
# storages.append(lazy_fixture.lf("azure_storage"))


@pytest.mark.parametrize(
    "storage",
    storages,
    indirect=True,
)
@pytest.mark.deploy_modes("standalone")
async def test_file_tus_upload_and_download(
    storage: Storage,
    configure_redis_dm,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox_one: str,
):
    language = "ca"
    filename = "image.jpeg"
    md5 = "7af0916dba8b70e29d99e72941923529"
    # aitable is a custom content type suffix to indicate
    # that the file must be processed with the ai tables feature...
    content_type = "image/jpeg+aitable"

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
    upload_metadata = ",".join(
        [
            f"filename {header_encode(filename)}",
            f"language {header_encode(language)}",
            f"md5 {header_encode(md5)}",
        ]
    )
    resp = await nucliadb_writer.post(
        url,
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": upload_metadata,
            "content-type": content_type,
            "upload-defer-length": "1",
        },
    )
    assert resp.status_code == 201, resp.json()
    # Get the URL to upload the file to
    url = resp.headers["location"]

    # Create a 2Mb file in memory
    mb = 1024 * 1024
    file_content = BytesIO(b"A" * 10 * mb)
    file_content.seek(0)

    # Upload the file part by part
    file_storage_manager = get_storage_manager()
    chunk_size = file_storage_manager.min_upload_size or file_storage_manager.chunk_size

    chunks_uploaded = 0
    offset = 0
    chunk = file_content.read(chunk_size)
    while chunk != b"":
        chunks_uploaded += 1

        # Make sure the upload is at the right offset
        resp = await nucliadb_writer.head(url, timeout=None)
        assert resp.headers["Upload-Length"] == f"0"
        assert resp.headers["Upload-Offset"] == f"{offset}"

        headers = {
            "upload-offset": f"{offset}",
            "content-length": f"{len(chunk)}",
        }
        if file_content.tell() == file_content.getbuffer().nbytes:
            # If this is the last part, we need to set the upload-length header
            headers["upload-length"] = f"{offset + len(chunk)}"

        # Upload the chunk
        resp = await nucliadb_writer.patch(
            url,
            content=chunk,
            headers=headers,
        )
        assert resp.status_code == 200
        offset += len(chunk)
        chunk = file_content.read(chunk_size)

    # Check that we tested at least with 2 chunks
    assert chunks_uploaded >= 2

    # Make sure the upload is finished on the server side
    assert resp.headers["Tus-Upload-Finished"] == "1"
    assert "NDB-Resource" in resp.headers
    assert "NDB-Field" in resp.headers

    # Now download the file
    download_url = f"{kb_path}/{RESOURCE_PREFIX}/{resource}/file/field1/download/field"
    resp = await nucliadb_reader.get(download_url, timeout=None)
    assert resp.status_code == 200, resp.text
    # Make sure the filename and contents are correct
    assert resp.headers["Content-Disposition"] == f'attachment; filename="{filename}"'
    assert resp.headers["Content-Type"] == content_type
    assert resp.content == file_content.getvalue()

    # Download the file with range headers
    range_downloaded = BytesIO()
    download_url = f"{kb_path}/{RESOURCE_PREFIX}/{resource}/file/field1/download/field"

    # One chunk first
    resp = await nucliadb_reader.get(
        download_url,
        headers={
            "Range": "bytes=0-100",
        },
    )
    assert resp.status_code == 206
    range_downloaded.write(resp.content)

    # Some more
    resp = await nucliadb_reader.get(
        download_url,
        headers={
            "Range": "bytes=101-200",
        },
    )
    assert resp.status_code == 206
    range_downloaded.write(resp.content)

    # The rest of the file
    resp = await nucliadb_reader.get(
        download_url,
        headers={
            "Range": "bytes=201-",
        },
    )
    assert resp.status_code == 206
    range_downloaded.write(resp.content)

    # Make sure the downloaded content is the same as the original file
    file_content.seek(0)
    range_downloaded.seek(0)
    assert file_content.getvalue() == range_downloaded.getvalue()

    # Test with a range that is too big
    resp = await nucliadb_reader.get(
        download_url,
        headers={
            "Range": "bytes=99900000-",
        },
    )
    assert resp.status_code == 416


@pytest.mark.parametrize(
    "storage",
    storages,
    indirect=True,
)
@pytest.mark.deploy_modes("standalone")
async def test_file_tus_supports_empty_files(
    storage: Storage,
    configure_redis_dm,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox_one: str,
):
    language = "ca"
    filename = "image.jpeg"
    content_type = "image/jpeg"
    upload_metadata = ",".join(
        [
            f"filename {header_encode(filename)}",
            f"language {header_encode(language)}",
        ]
    )
    # First test with deferred length
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox_one}/tusupload",
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": upload_metadata,
            "content-type": content_type,
            "upload-defer-length": "1",
        },
    )
    assert resp.status_code == 201, resp.json()
    url = resp.headers["location"]
    resp = await nucliadb_writer.patch(
        url,
        content=b"",
        headers={
            "upload-offset": "0",
            "content-length": "0",
            "upload-length": "0",
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.headers["Tus-Upload-Finished"] == "1"

    # Now test without deferred length
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox_one}/tusupload",
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": upload_metadata,
            "content-type": content_type,
            "upload-length": "0",
        },
    )
    assert resp.status_code == 201, resp.json()
    url = resp.headers["location"]

    resp = await nucliadb_writer.patch(
        url,
        content=b"",
        headers={
            "upload-offset": "0",
            "content-length": "0",
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.headers["Tus-Upload-Finished"] == "1"


@pytest.mark.deploy_modes("standalone")
async def test_tus_upload_handles_unknown_upload_ids(
    configure_redis_dm,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox_one: str,
):
    kbid = knowledgebox_one
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/{TUSUPLOAD}/foobarid",
        headers={},
        content=b"foobar",
    )
    assert resp.status_code == 404
    error_detail = resp.json().get("detail")
    assert error_detail == "Resumable URI not found for upload_id: foobarid"


@pytest.mark.parametrize(
    "storage",
    [
        lazy_fixture.lf("local_storage"),
    ],
    indirect=True,
)
@pytest.mark.deploy_modes("standalone")
async def test_content_type_validation(
    storage: Storage,
    configure_redis_dm,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox_one: str,
):
    language = "ca"
    filename = "image.jpg"
    md5 = "7af0916dba8b70e29d99e72941923529"

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
    upload_metadata = ",".join(
        [
            f"filename {header_encode(filename)}",
            f"language {header_encode(language)}",
            f"md5 {header_encode(md5)}",
        ]
    )
    resp = await nucliadb_writer.post(
        url,
        headers={
            "tus-resumable": "1.0.0",
            "upload-metadata": upload_metadata,
            "content-type": "invalid-content-type",
            "upload-defer-length": "1",
        },
    )
    assert resp.status_code == 415
    error_detail = resp.json().get("detail")
    assert error_detail == "Unsupported content type: invalid-content-type"


@pytest.mark.parametrize(
    "content_type",
    [
        "application/epub+zip",
        "application/font-woff",
        "application/generic",
        "application/java-archive",
        "application/java-vm",
        "application/json",
        "application/mp4",
        "application/msword",
        "application/octet-stream",
        "application/pdf",
        "application/pdf+aitable",
        "application/postscript",
        "application/rls-services+xml",
        "application/rtf",
        "application/stf-link",
        "application/toml",
        "application/vnd.jgraph.mxfile",
        "application/vnd.lotus-organizer",
        "application/vnd.ms-excel.sheet.macroenabled.12",
        "application/vnd.ms-excel",
        "application/vnd.ms-excel+aitable",
        "application/vnd.ms-outlook",
        "application/vnd.ms-powerpoint",
        "application/vnd.ms-project",
        "application/vnd.ms-word.document.macroenabled.12",
        "application/vnd.oasis.opendocument.presentation",
        "application/vnd.oasis.opendocument.text",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation+aitable",
        "application/vnd.openxmlformats-officedocument.presentationml.slideshow",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet+aitable",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document+aitable",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.template",
        "application/vnd.rar",
        "application/x-mobipocket-ebook",
        "application/x-ms-shortcut",
        "application/x-msdownload",
        "application/x-ndjson",
        "application/x-openscad",
        "application/x-sql",
        "application/x-zip-compressed",
        "application/xml",
        "application/zip",
        "application/zstd",
        "audio/aac",
        "audio/mp4",
        "audio/mpeg",
        "audio/vnd.dlna.adts",
        "audio/wav",
        "audio/x-m4a",
        "image/avif",
        "image/gif",
        "image/heic",
        "image/jpeg",
        "image/jpeg+aitable",
        "image/png",
        "image/png+aitable",
        "image/svg+xml",
        "image/tiff",
        "image/vnd.djvu",
        "image/vnd.dwg",
        "image/webp",
        "model/stl",
        "text/calendar",
        "text/css",
        "text/csv",
        "text/csv+aitable",
        "text/html",
        "text/javascript",
        "text/jsx",
        "text/markdown",
        "text/plain",
        "text/rtf",
        "text/rtf+aitable",
        "text/x-java-source",
        "text/x-log",
        "text/x-python",
        "text/x-ruby-script",
        "text/xml",
        "text/yaml",
        "video/mp4",
        "video/mp4+aitable",
        "video/quicktime",
        "video/webm",
        "video/x-m4v",
        "video/x-ms-wmv",
        "video/YouTube",
        "multipart/form-data",
        "text/plain+blankline",
    ],
)
def test_valid_content_types(content_type):
    assert content_types.valid(content_type)


@pytest.mark.parametrize(
    "content_type",
    [
        "multipart/form-data;boundary=--------------------------472719318099714047986957",
        "application/pdf+aitablexxx",
        "text/plain+blanklinesss",
    ],
)
def test_invalid_content_types(content_type):
    assert not content_types.valid(content_type)


@pytest.mark.parametrize(
    "filename,content_type",
    [
        # Text files
        ("foo.txt", "text/plain"),
        ("foo.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        ("foo.pdf", "application/pdf"),
        ("foo.json", "application/json"),
        # Spreadsheets
        ("foo.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        ("foo.csv", "text/csv"),
        # Presentations
        ("foo.pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        # Images
        ("image.jpg", "image/jpeg"),
        ("image.jpeg", "image/jpeg"),
        ("image.png", "image/png"),
        ("image.tiff", "image/tiff"),
        ("image.gif", "image/gif"),
        # Videos
        ("video.mp4", "video/mp4"),
        ("video.webm", "video/webm"),
        ("video.avi", "video/x-msvideo"),
        ("video.mpeg", "video/mpeg"),
        # Audio
        ("audio.mp3", "audio/mpeg"),
        ("audio.wav", "audio/x-wav"),
        # Web data
        ("data.html", "text/html"),
        ("data.xml", "application/xml"),
        # Archive files
        ("archive.zip", "application/zip"),
        ("fooo.rar", ["application/x-rar-compressed", "application/vnd.rar"]),
        ("archive.tar", "application/x-tar"),
        ("archive.tar.gz", "application/x-tar"),
        # Invalid content types
        ("foobar", None),
        ("someuuidwithoutextension", None),
        ("", None),
    ],
)
def test_guess_content_type(filename, content_type):
    if isinstance(content_type, list):
        assert content_types.guess(filename) in content_type
    else:
        assert content_types.guess(filename) == content_type
