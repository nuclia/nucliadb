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
import hashlib

import pytest
from httpx import AsyncClient

from nucliadb.writer.tus import UPLOAD


@pytest.mark.deploy_modes("standalone")
async def test_upload(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    content = b"Test for /upload endpoint"
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/{UPLOAD}",
        headers={
            "X-Filename": base64.b64encode(b"testfile").decode("utf-8"),
            "Content-Type": "text/plain",
            "Content-Length": str(len(content)),
        },
        content=content,
    )
    if resp.status_code == 500:
        print(resp.content)
    assert resp.status_code == 201
    body = resp.json()

    seqid = body["seqid"]
    rid = body["uuid"]
    field_id = body["field_id"]
    assert seqid is not None
    assert rid
    assert field_id

    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/resource/{rid}/file/{field_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["value"]["file"]["filename"] == "testfile"
    download_uri = body["value"]["file"]["uri"]

    resp = await nucliadb_reader.get(download_uri)
    assert resp.status_code == 200
    assert resp.content == content


@pytest.mark.deploy_modes("standalone")
async def test_upload_guesses_content_type(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    filename = "testfile.txt"
    content = b"Test for /upload endpoint"
    content_type = "text/plain"
    # Upload the file without specifying the content type
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/{UPLOAD}",
        headers={
            "X-Filename": base64.b64encode(filename.encode()).decode("utf-8"),
        },
        content=base64.b64encode(content),
    )
    assert resp.status_code == 201
    body = resp.json()
    rid = body["uuid"]
    field_id = body["field_id"]

    # Test that the content type is correctly guessed from the filename
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/resource/{rid}/file/{field_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["value"]["file"]["filename"] == filename
    assert body["value"]["file"]["content_type"] == content_type


@pytest.mark.deploy_modes("standalone")
async def test_upload_checks_duplicates(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    content = b"Test for /upload endpoint"
    filename = "testfile.txt"
    encoded_filename = base64.b64encode(filename.encode()).decode("utf-8")
    content_type = "text/plain"
    content_length = str(len(content))
    md5 = hashlib.md5(content).hexdigest()

    # Upload the file for the first time
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/{UPLOAD}",
        headers={
            "X-Filename": encoded_filename,
            "X-MD5": md5,
            "Content-Type": content_type,
            "Content-Length": content_length,
        },
        content=content,
    )
    assert resp.status_code == 201

    # Uploading the file twice should return 409 Conflict
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/{UPLOAD}",
        headers={
            "X-Filename": encoded_filename,
            "X-MD5": md5,
            "Content-Type": content_type,
            "Content-Length": content_length,
        },
        content=content,
    )
    assert resp.status_code == 409

    # Check now the field upload endpoint
    # Create an empty resource first
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": "Test Resource",
        },
    )
    resp.raise_for_status()
    rid = resp.json()["uuid"]

    # Upload the file to the resource
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/file/file/upload",
        headers={
            "X-Filename": encoded_filename,
            "X-MD5": md5,
            "Content-Type": content_type,
            "Content-Length": content_length,
        },
        content=content,
    )
    resp.raise_for_status()

    # Uploading the file to the same field again should not return 409 Conflict, as we're overwriting the field
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resource/{rid}/file/file/upload",
        headers={
            "X-Filename": encoded_filename,
            "X-MD5": md5,
            "Content-Type": content_type,
            "Content-Length": content_length,
        },
        content=content,
    )
    resp.raise_for_status()
