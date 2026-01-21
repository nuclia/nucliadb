# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
    create_export = sdk.start_export(kbid=src_kbid)
    export_id = create_export.export_id
    assert sdk.export_status(kbid=src_kbid, export_id=export_id).status == "finished"
    export_generator = sdk.download_export(kbid=src_kbid, export_id=export_id)

    # Import to dst kb
    create_import = sdk.start_import(kbid=dst_kbid, content=export_generator(chunk_size=1024))
    import_id = create_import.import_id
    assert sdk.import_status(kbid=dst_kbid, import_id=import_id).status == "finished"

    _check_kbs_are_equal(sdk, src_kbid, dst_kbid)


def test_export_import_with_files(src_kbid, dst_kbid, sdk: nucliadb_sdk.NucliaDB):
    # Export src kb
    create_export = sdk.start_export(kbid=src_kbid)
    export_id = create_export.export_id
    assert sdk.export_status(kbid=src_kbid, export_id=export_id).status == "finished"
    export_generator = sdk.download_export(kbid=src_kbid, export_id=export_id)

    # Store in a file
    with NamedTemporaryFile() as f:
        for chunk in export_generator():
            f.write(chunk)

        # Import to dst kb from a file
        create_import = sdk.start_import(kbid=dst_kbid, content=open(f.name, "rb"))
        import_id = create_import.import_id
        assert sdk.import_status(kbid=dst_kbid, import_id=import_id).status == "finished"

    _check_kbs_are_equal(sdk, src_kbid, dst_kbid)


async def test_export_import_kb_async(src_kbid, dst_kbid, sdk, sdk_async: nucliadb_sdk.NucliaDBAsync):
    # Export src kb
    create_export = await sdk_async.start_export(kbid=src_kbid)
    export_id = create_export.export_id
    assert (await sdk_async.export_status(kbid=src_kbid, export_id=export_id)).status == "finished"
    export_generator = sdk_async.download_export(kbid=src_kbid, export_id=export_id)

    # Import to dst kb
    create_import = await sdk_async.start_import(kbid=dst_kbid, content=export_generator(1024))
    import_id = create_import.import_id
    assert (await sdk_async.import_status(kbid=dst_kbid, import_id=import_id)).status == "finished"

    _check_kbs_are_equal(sdk, src_kbid, dst_kbid)


def _check_kbs_are_equal(sdk: nucliadb_sdk.NucliaDB, kb1: str, kb2: str):
    kb1_resources = sdk.list_resources(kbid=kb1)
    kb2_resources = sdk.list_resources(kbid=kb2)
    assert len(kb1_resources.resources) == len(kb2_resources.resources)

    kb1_labels = sdk.get_labelsets(kbid=kb1)
    kb2_labels = sdk.get_labelsets(kbid=kb2)
    assert kb1_labels.labelsets == kb2_labels.labelsets
