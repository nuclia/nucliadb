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
#
import nucliadb_sdk
from nucliadb_models.kv_schemas import KBKVSchemas, KVFieldType, KVSchema, KVSchemaField, UpdateKVSchema

PRODUCT_SCHEMA = KVSchema(
    id="product",
    description="A product schema",
    fields=[
        KVSchemaField(key="color", type=KVFieldType.TEXT),
        KVSchemaField(key="price", type=KVFieldType.FLOAT),
        KVSchemaField(key="in_stock", type=KVFieldType.BOOLEAN),
    ],
)

UPDATED_SCHEMA = UpdateKVSchema(
    description="Updated product schema",
    fields=[
        KVSchemaField(key="color", type=KVFieldType.TEXT),
        KVSchemaField(key="price", type=KVFieldType.FLOAT),
        KVSchemaField(key="in_stock", type=KVFieldType.BOOLEAN),
        KVSchemaField(key="quantity", type=KVFieldType.INTEGER, required=False),
    ],
)


def test_kv_schemas_crud(sdk: nucliadb_sdk.NucliaDB, kb):
    kbid = kb.uuid

    # List returns empty at start
    result = sdk.list_kv_schemas(kbid=kbid)
    assert isinstance(result, KBKVSchemas)
    assert result.schemas == {}

    # Create
    created = sdk.create_kv_schema(kbid=kbid, content=PRODUCT_SCHEMA)
    assert isinstance(created, KVSchema)
    assert created.id == "product"
    assert created.description == "A product schema"
    assert len(created.fields) == 3

    # List
    result = sdk.list_kv_schemas(kbid=kbid)
    assert "product" in result.schemas

    # Get
    fetched = sdk.get_kv_schema(kbid=kbid, schema_id="product")
    assert isinstance(fetched, KVSchema)
    assert fetched.id == "product"

    # Update
    updated = sdk.update_kv_schema(kbid=kbid, schema_id="product", content=UPDATED_SCHEMA)
    assert isinstance(updated, KVSchema)
    assert updated.description == "Updated product schema"
    assert len(updated.fields) == 4

    # Delete
    sdk.delete_kv_schema(kbid=kbid, schema_id="product")
    result = sdk.list_kv_schemas(kbid=kbid)
    assert result.schemas == {}


async def test_kv_schemas_crud_async(sdk_async: nucliadb_sdk.NucliaDBAsync, kb):
    kbid = kb.uuid

    result = await sdk_async.list_kv_schemas(kbid=kbid)
    assert isinstance(result, KBKVSchemas)
    assert result.schemas == {}

    created = await sdk_async.create_kv_schema(kbid=kbid, content=PRODUCT_SCHEMA)
    assert isinstance(created, KVSchema)
    assert created.id == "product"

    result = await sdk_async.list_kv_schemas(kbid=kbid)
    assert "product" in result.schemas

    fetched = await sdk_async.get_kv_schema(kbid=kbid, schema_id="product")
    assert isinstance(fetched, KVSchema)

    updated = await sdk_async.update_kv_schema(kbid=kbid, schema_id="product", content=UPDATED_SCHEMA)
    assert updated.description == "Updated product schema"
    assert len(updated.fields) == 4

    await sdk_async.delete_kv_schema(kbid=kbid, schema_id="product")
    result = await sdk_async.list_kv_schemas(kbid=kbid)
    assert result.schemas == {}
