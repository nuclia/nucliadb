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

import pytest

import nucliadb_sdk
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_models.search import ResourceProperties


def test_crud_resource(kb: KnowledgeBoxObj, sdk: nucliadb_sdk.NucliaDB):
    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError):
        sdk.get_resource_by_slug(kbid=kb.uuid, slug="mykey1")

    sdk.create_resource(
        kbid=kb.uuid,
        texts={"text": {"body": "I'm Ramon"}},
        slug="mykey1",
        files={
            "file": {
                "file": {
                    "filename": "data",
                    "payload": base64.b64encode(b"asd"),
                }
            }
        },
        usermetadata={
            "classifications": [{"labelset": "labelset", "label": "positive"}]
        },
        fieldmetadata=[
            {
                "field": {
                    "field": "text",
                    "field_type": "text",
                },
                "token": [{"token": "Ramon", "klass": "NAME", "start": 5, "end": 9}],
            }
        ],
        uservectors=[
            {
                "field": {
                    "field": "text",
                    "field_type": "text",
                },
                "vectors": {
                    "base": {
                        "vectors": {"vector": [1.0, 0.2]},
                    }
                },
            }
        ],
    )
    resource = sdk.get_resource_by_slug(
        kbid=kb.uuid,
        slug="mykey1",
        query_params={
            "show": [ResourceProperties.BASIC.value, ResourceProperties.VALUES.value]
        },
    )
    assert resource.data is not None
    assert resource.data.texts is not None
    assert resource.data.texts["text"].value.body == "I'm Ramon"
    assert resource.data.files is not None
    assert resource.data.files["file"].value.file.filename == "data"

    sdk.update_resource(
        kbid=kb.uuid,
        rid=resource.id,
        texts={"text": {"body": "I'm an updated Ramon"}},
    )

    resource = sdk.get_resource_by_slug(
        kbid=kb.uuid,
        slug="mykey1",
        query_params={
            "show": [ResourceProperties.BASIC.value, ResourceProperties.VALUES.value]
        },
    )
    assert resource.data.texts["text"].value.body == "I'm an updated Ramon"

    sdk.delete_resource(kbid=kb.uuid, rid=resource.id)

    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError):
        sdk.get_resource_by_slug(kbid=kb.uuid, slug="mykey1")
