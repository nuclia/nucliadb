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

import base64
import tarfile
import tempfile
from io import StringIO
from typing import List, Optional

import pytest
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb_client.client import NucliaDBClient
from nucliadb_client.exceptions import ConflictError
from nucliadb_client.knowledgebox import KnowledgeBox
from nucliadb_models.common import File
from nucliadb_models.file import FileField
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdString
from nucliadb_models.writer import CreateResourcePayload


@pytest.mark.asyncio
async def test_export_import(nucliadb_client: NucliaDBClient):
    nucliadb_client.init_async_grpc()
    exists = nucliadb_client.get_kb(slug="src")
    if exists:
        exists.delete()

    try:
        srckb: Optional[KnowledgeBox] = nucliadb_client.create_kb(
            title="My KB", description="Its a new KB", slug="src"
        )
    except ConflictError:
        srckb = nucliadb_client.get_kb(slug="src")
    if srckb is None:
        raise Exception("Not found source KB")

    file_binary = base64.b64encode(b"Hola")
    payload = CreateResourcePayload()
    payload.icon = "plain/text"
    payload.title = "My Resource"
    payload.summary = "My long summary of the resource"
    payload.slug = "myresource"  # type: ignore
    payload.texts[FieldIdString("text1")] = TextField(body="My text")
    payload.files[FieldIdString("file1")] = FileField(
        file=File(
            filename="filename.png",
            content_type="image/png",
            payload=file_binary.decode(),
            md5="XXX",
        )
    )
    resource = srckb.create_resource(payload)
    assert resource.download_file("file1").content == file_binary

    # Export data
    export = []
    binaries: List[CloudFile] = []
    async for line in srckb.generator(binaries):
        export.append(line)
    assert len(binaries) == 1
    data = StringIO("\n".join(export))

    # Create binaries tar
    with tempfile.TemporaryDirectory() as tmp:
        filename = f"{tmp}/foo"
        binaries_tar = "binaries.tar.bz2"
        with tarfile.open(binaries_tar, mode="w:bz2") as tar:
            for cf in binaries:
                await srckb.download_file(cf, filename)
                tar.add(filename, cf.uri)

    exists = nucliadb_client.get_kb(slug="dst")
    if exists:
        exists.delete()

    # Import exported data dump and binaries
    await nucliadb_client.import_kb(slug="dst", location=data)
    dstkb = nucliadb_client.get_kb(slug="dst")
    if dstkb is None:
        raise AttributeError("Could not find destination KB")
    await dstkb.import_tar_bz2(binaries_tar)

    resources = dstkb.list_resources()

    bm = BrokerMessage()
    bm.ParseFromString(base64.b64decode(export[0][4:]))
    assert bm.basic.title == resources[0].get().title

    counters = dstkb.counters()
    assert counters
    assert counters.resources == 1

    found = False
    for resource in dstkb.iter_resources(page_size=1):
        found = True
        res = resource.get(show=["values", "basic"])
        assert res.id == bm.uuid
        assert res.title == bm.basic.title
        assert res.slug == resource.slug == bm.basic.slug == payload.slug
        assert resource.download_file("file1") == file_binary
    assert found


@pytest.mark.asyncio
<<<<<<< HEAD
async def test_export_import_e2e(nucliadb_client):
=======
async def test_export_import_e2e_local(nucliadb_client: NucliaDBClient):
    await export_import_e2e_test(nucliadb_client)


@pytest.mark.asyncio
async def test_export_import_e2e_gcs(nucliadb_client_gcs: NucliaDBClient):
    await export_import_e2e_test(nucliadb_client_gcs)


async def export_import_e2e_test(nucliadb_client: NucliaDBClient):
>>>>>>> 24b2cdde (Turn dm.update into a regular function, as it is not using I/O)
    nucliadb_client.init_async_grpc()
    for slug in ("src1", "dst1"):
        exists = nucliadb_client.get_kb(slug=slug)
        if exists:
            exists.delete()

    try:
        srckb: Optional[KnowledgeBox] = nucliadb_client.create_kb(
            title="My KB", description="Its a new KB", slug="src1"
        )
    except ConflictError:
        srckb = nucliadb_client.get_kb(slug="src1")
    if srckb is None:
        raise Exception("Could not create source KB")

    file_binary = base64.b64encode(b"Hola")
    payload = CreateResourcePayload()
    payload.icon = "plain/text"
    payload.title = "My Resource"
    payload.summary = "My long summary of the resource"
    payload.slug = "myresource"  # type: ignore
    payload.texts[FieldIdString("text1")] = TextField(body="My text")
    payload.files[FieldIdString("file1")] = FileField(
        file=File(
            filename="filename.png",
            content_type="image/png",
            payload=file_binary.decode(),
            md5="XXX",
        )
    )
    resource = srckb.create_resource(payload)

    # TUS Upload
    file2_binary = "Sayonara".encode()
    file2 = FileField(
        file=File(
            filename="mypdf.pdf",
            content_type="application/pdf",
            payload=file2_binary,
            md5="YYY",
        )
    )
    resource.tus_upload_file("file2", file2, wait=True)

    # Regular Upload
    file3_binary = "".encode()
    file3 = FileField(
        file=File(
            filename="my_image.png",
            content_type="image/png",
            payload=file3_binary,
            md5="YYY",
        )
    )
    resource.upload_file("file3", file3, wait=True)

    with tempfile.NamedTemporaryFile() as dump:
        await nucliadb_client.export_kb(kbid=srckb.kbid, location=dump.name)
        await nucliadb_client.import_kb(slug="dst1", location=dump.name)

    dstkb = nucliadb_client.get_kb(slug="dst1")
    assert dstkb is not None

    found = False
    for resource in dstkb.iter_resources(page_size=1):
        found = True
        res = resource.get(show=["values", "basic"])
        assert res.id == resource.rid
        assert res.title == payload.title
        assert res.slug == payload.slug
        assert resource.download_file("file1") == file_binary
    assert found

    # Test search results are equal
    src_search = srckb.search(query="")
    dst_search = dstkb.search(query="")

    # Resources
    assert len(src_search.resources) == len(dst_search.resources)
    for rid, sresult in src_search.resources.items():
        dresult = dst_search.resources[rid]
        assert sresult.slug == dresult.slug
        assert sresult.summary == dresult.summary
        assert sresult.icon == dresult.icon

    # Fulltext
    assert len(src_search.fulltext.results) == len(dst_search.fulltext.results)
    src_fulltext = {
        (ftr.rid, ftr.field_type, ftr.field, ftr.score)
        for ftr in src_search.fulltext.results
    }
    dst_fulltext = {
        (ftr.rid, ftr.field_type, ftr.field, ftr.score)
        for ftr in dst_search.fulltext.results
    }
    assert src_fulltext == dst_fulltext

    # Paragraphs
    assert len(src_search.paragraphs.results) == len(dst_search.paragraphs.results)
    src_presults = {
        (par.score, par.rid, par.field_type, par.field, par.text, par.position.json())
        for par in src_search.paragraphs.results
    }
    dst_presults = {
        (par.score, par.rid, par.field_type, par.field, par.text, par.position.json())
        for par in dst_search.paragraphs.results
    }
    assert src_presults == dst_presults
