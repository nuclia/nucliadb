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

from io import BytesIO
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
    export_generator = sdk.export_knowledge_box(kbid=src_kbid)
    export = BytesIO()
    for chunk in export_generator:
        export.write(chunk)
    export.seek(0)
    assert len(export.getvalue()) > 0

    sdk.import_knowledge_box(dst_kbid, file=export)

    _check_kbs_are_equal(sdk, src_kbid, dst_kbid)


def _check_kbs_are_equal(sdk: nucliadb_sdk.NucliaDB, kb1: str, kb2: str):
    kb1_resources = sdk.get_resources(kbid=kb1)
    kb2_resources = sdk.get_resources(kbid=kb2)
    assert len(kb1_resources.resources) == len(kb2_resources.resources)

    kb1_entities = sdk.get_entitygroups(kbid=kb1, show_entities=True)
    kb2_entities = sdk.get_entitygroups(kbid=kb2, show_entities=True)
    assert kb1_entities == kb2_entities

    kb1_labels = sdk.get_labelsets(kbid=kb1)
    kb2_labels = sdk.get_labelsets(kbid=kb2)
    assert kb1_labels == kb2_labels
