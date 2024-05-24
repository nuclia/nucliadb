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

from tempfile import NamedTemporaryFile
from uuid import uuid4

import pytest

import nucliadb_sdk


@pytest.fixture(scope="function")
def src_kbid(docs_dataset: str):
    yield docs_dataset


@pytest.fixture(scope="function")
def dst_kbid(sdk: nucliadb_sdk.NucliaDB):
    kbslug = uuid4().hex
    kb = sdk.create_knowledge_box(slug=kbslug)

    yield kb.uuid

    sdk.delete_knowledge_box(kbid=kb.uuid)


def test_export_import_kb(src_kbid, dst_kbid, sdk: nucliadb_sdk.NucliaDB):
    # Export src kb
    resp = sdk.start_export(kbid=src_kbid)
    export_id = resp.export_id
    assert sdk.export_status(kbid=src_kbid, export_id=export_id).status == "finished"
    export_generator = sdk.download_export(kbid=src_kbid, export_id=export_id)

    # Import to dst kb
    resp = sdk.start_import(kbid=dst_kbid, content=export_generator(chunk_size=1024))
    import_id = resp.import_id
    assert sdk.import_status(kbid=dst_kbid, import_id=import_id).status == "finished"

    _check_kbs_are_equal(sdk, src_kbid, dst_kbid)


def test_export_import_with_files(src_kbid, dst_kbid, sdk: nucliadb_sdk.NucliaDB):
    # Export src kb
    resp = sdk.start_export(kbid=src_kbid)
    export_id = resp.export_id
    assert sdk.export_status(kbid=src_kbid, export_id=export_id).status == "finished"
    export_generator = sdk.download_export(kbid=src_kbid, export_id=export_id)

    # Store in a file
    with NamedTemporaryFile() as f:
        for chunk in export_generator():
            f.write(chunk)

        # Import to dst kb from a file
        resp = sdk.start_import(kbid=dst_kbid, content=open(f.name, "rb"))
        import_id = resp.import_id
        assert (
            sdk.import_status(kbid=dst_kbid, import_id=import_id).status == "finished"
        )

    _check_kbs_are_equal(sdk, src_kbid, dst_kbid)


async def test_export_import_kb_async(
    src_kbid, dst_kbid, sdk, sdk_async: nucliadb_sdk.NucliaDB
):
    # Export src kb
    resp = await sdk_async.start_export(kbid=src_kbid)
    export_id = resp.export_id
    assert (
        await sdk_async.export_status(kbid=src_kbid, export_id=export_id)
    ).status == "finished"
    export_generator = sdk_async.download_export(kbid=src_kbid, export_id=export_id)

    # Import to dst kb
    resp = await sdk_async.start_import(
        kbid=dst_kbid, content=export_generator(chunk_size=1024)
    )
    import_id = resp.import_id
    assert (
        await sdk_async.import_status(kbid=dst_kbid, import_id=import_id)
    ).status == "finished"

    _check_kbs_are_equal(sdk, src_kbid, dst_kbid)


def _check_kbs_are_equal(sdk: nucliadb_sdk.NucliaDB, kb1: str, kb2: str):
    kb1_resources = sdk.list_resources(kbid=kb1)
    kb2_resources = sdk.list_resources(kbid=kb2)
    assert len(kb1_resources.resources) == len(kb2_resources.resources)

    kb1_entities = sdk.get_entitygroups(kbid=kb1)
    kb2_entities = sdk.get_entitygroups(kbid=kb2)
    assert kb1_entities.groups == kb2_entities.groups

    kb1_labels = sdk.get_labelsets(kbid=kb1)
    kb2_labels = sdk.get_labelsets(kbid=kb2)
    assert kb1_labels.labelsets == kb2_labels.labelsets
