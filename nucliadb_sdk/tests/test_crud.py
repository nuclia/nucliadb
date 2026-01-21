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
        usermetadata={"classifications": [{"labelset": "labelset", "label": "positive"}]},
        fieldmetadata=[
            {
                "field": {
                    "field": "text",
                    "field_type": "text",
                },
            }
        ],
    )
    resource = sdk.get_resource_by_slug(
        kbid=kb.uuid,
        slug="mykey1",
        query_params={"show": [ResourceProperties.BASIC.value, ResourceProperties.VALUES.value]},
    )
    assert resource.data is not None
    assert resource.data.texts is not None
    assert resource.data.texts["text"].value is not None
    assert resource.data.texts["text"].value.body == "I'm Ramon"
    assert resource.data.files is not None
    assert resource.data.files["file"].value is not None
    assert resource.data.files["file"].value.file is not None
    assert resource.data.files["file"].value.file.filename == "data"

    sdk.update_resource(
        kbid=kb.uuid,
        rid=resource.id,
        texts={"text": {"body": "I'm an updated Ramon"}},
    )

    resource = sdk.get_resource_by_slug(
        kbid=kb.uuid,
        slug="mykey1",
        query_params={"show": [ResourceProperties.BASIC.value, ResourceProperties.VALUES.value]},
    )
    assert resource.data is not None
    assert resource.data.texts is not None
    assert resource.data.texts["text"].value is not None
    assert resource.data.texts["text"].value.body == "I'm an updated Ramon"

    sdk.delete_resource_by_slug(kbid=kb.uuid, rslug="mykey1")

    with pytest.raises(nucliadb_sdk.exceptions.NotFoundError):
        sdk.get_resource_by_slug(kbid=kb.uuid, slug="mykey1")


def test_modify_resource_slug(kb: KnowledgeBoxObj, sdk: nucliadb_sdk.NucliaDB):
    sdk.create_resource(kbid=kb.uuid, texts={"text": {"body": "I'm Ramon"}}, slug="my-resource")
    resource = sdk.get_resource_by_slug(
        kbid=kb.uuid,
        slug="my-resource",
    )
    assert resource.slug == "my-resource"

    # By uuid
    sdk.update_resource(kbid=kb.uuid, rid=resource.id, slug="my-new-slug")
    resource = sdk.get_resource_by_slug(
        kbid=kb.uuid,
        slug="my-new-slug",
    )
    assert resource.slug == "my-new-slug"

    # Now by slug
    sdk.update_resource_by_slug(kbid=kb.uuid, rslug="my-new-slug", slug="my-next-new-slug")
    resource = sdk.get_resource_by_slug(
        kbid=kb.uuid,
        slug="my-next-new-slug",
    )
    assert resource.slug == "my-next-new-slug"
