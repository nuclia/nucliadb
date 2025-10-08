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
from os.path import dirname, getsize
from typing import Optional
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.fields.text import Text
from nucliadb.ingest.orm.knowledgebox import (
    KnowledgeBox,
)
from nucliadb_protos.knowledgebox_pb2 import VectorSetConfig
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    ExtractedVectorsWrapper,
    FieldID,
    FieldType,
)
from nucliadb_protos.utils_pb2 import Vector, VectorObject, Vectors
from nucliadb_utils.storages.azure import AzureStorage
from nucliadb_utils.storages.exceptions import CouldNotCopyNotFound
from nucliadb_utils.storages.gcs import GCSStorage
from nucliadb_utils.storages.local import LocalStorage
from nucliadb_utils.storages.s3 import S3Storage
from nucliadb_utils.storages.storage import Storage


async def test_create_resource_orm_vector(
    storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None
    vectorset_id = "my-semantic-model"
    storage_key_kind = VectorSetConfig.StorageKeyKind.LEGACY

    ex1 = ExtractedVectorsWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field="text1"))
    ex1.vectorset_id = vectorset_id
    v1 = Vector(start=1, end=2, vector=b"ansjkdn")
    ex1.vectors.vectors.vectors.append(v1)

    field_obj: Text = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_vectors(ex1, vectorset_id, storage_key_kind)

    ex2: Optional[VectorObject] = await field_obj.get_vectors(vectorset_id, storage_key_kind)
    assert ex2 is not None
    assert ex2.vectors.vectors[0].vector == ex1.vectors.vectors.vectors[0].vector


async def test_create_resource_orm_vector_file(
    local_files, storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None
    vectorset_id = "my-semantic-model"
    storage_key_kind = VectorSetConfig.StorageKeyKind.LEGACY

    ex1 = ExtractedVectorsWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field="text1"))
    ex1.vectorset_id = vectorset_id

    filename = f"{dirname(__file__)}/assets/vectors.pb"
    cf1 = CloudFile(
        uri="vectors.pb",
        source=CloudFile.Source.LOCAL,
        size=getsize(filename),
        bucket_name="/integration/orm/assets",
        content_type="application/octet-stream",
        filename="vectors.pb",
    )
    ex1.file.CopyFrom(cf1)

    field_obj: Text = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_vectors(ex1, vectorset_id, storage_key_kind)

    ex2: Optional[VectorObject] = await field_obj.get_vectors(vectorset_id, storage_key_kind)
    ex3 = VectorObject()
    with open(filename, "rb") as testfile:
        data2 = testfile.read()
    ex3.ParseFromString(data2)
    assert ex2 is not None

    assert ex3.vectors.vectors[0].vector == ex2.vectors.vectors[0].vector


async def test_create_resource_orm_vector_split(
    storage: Storage, txn, cache, dummy_nidx_utility, knowledgebox: str
):
    uuid = str(uuid4())
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)
    r = await kb_obj.add_resource(uuid=uuid, slug="slug")
    assert r is not None
    vectorset_id = "my-semantic-model"
    storage_key_kind = VectorSetConfig.StorageKeyKind.LEGACY

    ex1 = ExtractedVectorsWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.CONVERSATION, field="text1"))
    ex1.vectorset_id = vectorset_id
    v1 = Vector(start=1, vector=b"ansjkdn")
    vs1 = Vectors()
    vs1.vectors.append(v1)
    ex1.vectors.split_vectors["es1"].vectors.append(v1)
    ex1.vectors.split_vectors["es2"].vectors.append(v1)

    field_obj: Conversation = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj.set_vectors(ex1, vectorset_id, storage_key_kind)

    ex1 = ExtractedVectorsWrapper()
    ex1.field.CopyFrom(FieldID(field_type=FieldType.CONVERSATION, field="text1"))
    ex1.vectorset_id = vectorset_id
    v1 = Vector(start=1, vector=b"ansjkdn")
    vs1 = Vectors()
    vs1.vectors.append(v1)
    ex1.vectors.split_vectors["es3"].vectors.append(v1)
    ex1.vectors.split_vectors["es2"].vectors.append(v1)

    field_obj2: Text = await r.get_field(ex1.field.field, ex1.field.field_type, load=False)
    await field_obj2.set_vectors(ex1, vectorset_id, storage_key_kind)

    ex2: Optional[VectorObject] = await field_obj2.get_vectors(vectorset_id, storage_key_kind)
    assert ex2 is not None
    assert len(ex2.split_vectors) == 3


@pytest.mark.parametrize(
    "cf_uri,storage_key_kind,destination_path",
    [
        # legacy situation, processing stores without vectorset and we have a
        # legacy KB (with 1 vectorset)
        (
            "/kbs/{kbid}/r/{rid}/e/t/{field_id}/extracted_vectors",
            VectorSetConfig.StorageKeyKind.LEGACY,
            "kbs/{kbid}/r/{rid}/e/t/{field_id}/extracted_vectors",
        ),
        # New KBs in nucliadb with legacy processing
        (
            "/kbs/{kbid}/r/{rid}/e/t/{field_id}/extracted_vectors",
            VectorSetConfig.StorageKeyKind.VECTORSET_PREFIX,
            "kbs/{kbid}/r/{rid}/e/t/{field_id}/en-2024-04-24/extracted_vectors",
        ),
        # Legacy KBs but processing stores vectors in the new prefixed key
        (
            "/kbs/{kbid}/r/{rid}/e/t/{field_id}/en-2024-04-24/extracted_vectors",
            VectorSetConfig.StorageKeyKind.LEGACY,
            "kbs/{kbid}/r/{rid}/e/t/{field_id}/extracted_vectors",
        ),
        # KBs with vectorsets or new KBs with processing using prefix
        (
            "/kbs/{kbid}/r/{rid}/e/t/{field_id}/en-2024-04-24/extracted_vectors",
            VectorSetConfig.StorageKeyKind.VECTORSET_PREFIX,
            "kbs/{kbid}/r/{rid}/e/t/{field_id}/en-2024-04-24/extracted_vectors",
        ),
    ],
)
async def test_create_resource_with_cloud_file_vectors(
    cf_uri: str,
    storage_key_kind: VectorSetConfig.StorageKeyKind.ValueType,
    destination_path: str,
    storage: Storage,
    txn,
    dummy_nidx_utility,
    knowledgebox: str,
):
    vectorset_id = "en-2024-04-24"
    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)

    rid = str(uuid4())
    resource = await kb_obj.add_resource(uuid=rid, slug="slug")
    assert resource is not None

    field_id = "my-text"

    cf_uri = cf_uri.format(kbid=knowledgebox, rid=rid, field_id=field_id)
    destination_path = destination_path.format(kbid=knowledgebox, rid=rid, field_id=field_id)

    evw = ExtractedVectorsWrapper()
    evw.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field=field_id))
    evw.vectorset_id = vectorset_id
    storage_map = {
        AzureStorage: CloudFile.Source.AZURE,
        GCSStorage: CloudFile.Source.GCS,
        LocalStorage: CloudFile.Source.LOCAL,
        S3Storage: CloudFile.Source.S3,
    }
    evw.file.source = storage_map[type(storage)]
    evw.file.uri = cf_uri

    field_obj: Field = await resource.get_field(evw.field.field, evw.field.field_type, load=False)

    with patch.object(storage, "move") as move_mock:
        await field_obj.set_vectors(evw, vectorset_id, storage_key_kind)
        assert move_mock.call_count == 1
        cf, sf = move_mock.call_args.args
        assert cf.uri == cf_uri
        assert sf.key == destination_path


@pytest.mark.parametrize("found_in_storage", [True, False])
async def test_create_resource_with_cloud_file_vectors_already_moved(
    found_in_storage: bool,
    storage: Storage,
    txn,
    dummy_nidx_utility,
    knowledgebox: str,
):
    vectorset_id = "en-2024-04-24"
    cf_uri = "/kbs/{kbid}/r/{rid}/e/t/{field_id}/en-2024-04-24/extracted_vectors"
    storage_key_kind = VectorSetConfig.StorageKeyKind.LEGACY
    destination_path = "kbs/{kbid}/r/{rid}/e/t/{field_id}/extracted_vectors"

    kb_obj = KnowledgeBox(txn, storage, kbid=knowledgebox)

    rid = str(uuid4())
    resource = await kb_obj.add_resource(uuid=rid, slug="slug")
    assert resource is not None

    field_id = "my-text"

    cf_uri = cf_uri.format(kbid=knowledgebox, rid=rid, field_id=field_id)
    destination_path = destination_path.format(kbid=knowledgebox, rid=rid, field_id=field_id)

    evw = ExtractedVectorsWrapper()
    evw.field.CopyFrom(FieldID(field_type=FieldType.TEXT, field=field_id))
    evw.vectorset_id = vectorset_id
    storage_map = {
        AzureStorage: CloudFile.Source.AZURE,
        GCSStorage: CloudFile.Source.GCS,
        LocalStorage: CloudFile.Source.LOCAL,
        S3Storage: CloudFile.Source.S3,
    }
    evw.file.source = storage_map[type(storage)]
    evw.file.uri = cf_uri

    field_obj: Field = await resource.get_field(evw.field.field, evw.field.field_type, load=False)

    sf = AsyncMock()
    with (
        patch.object(
            field_obj.storage,
            "normalize_binary",
            side_effect=CouldNotCopyNotFound(
                cf_uri,
                "origin_bucket_name",
                destination_path,
                "destination_bucket_name",
                "<storage error>",
            ),
        ),
        patch.object(
            field_obj.storage,
            "download_pb",
            return_value=VectorObject(),
        ),
        patch.object(
            field_obj,
            "_get_extracted_vectors_storage_field",
            return_value=sf,
        ),
    ):
        if found_in_storage:
            sf.exists.return_value = True
            await field_obj.set_vectors(evw, vectorset_id, storage_key_kind)
        else:
            sf.exists.return_value = False
            with pytest.raises(CouldNotCopyNotFound):
                await field_obj.set_vectors(evw, vectorset_id, storage_key_kind)
