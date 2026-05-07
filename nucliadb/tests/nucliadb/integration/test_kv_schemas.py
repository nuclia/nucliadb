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

import pytest
from httpx import AsyncClient

from nucliadb_models.kv_schemas import MAX_KV_SCHEMAS

PRODUCT_SCHEMA = {
    "name": "product",
    "description": "A product schema",
    "fields": [
        {"key": "color", "type": "text", "description": "Product color", "required": True},
        {"key": "price", "type": "float", "description": "Product price", "required": True},
        {"key": "in_stock", "type": "boolean", "required": False},
        {"key": "quantity", "type": "integer", "required": False},
    ],
}


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_limit(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    for i in range(MAX_KV_SCHEMAS):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/kv-schemas",
            json={"name": f"schema{i}", "fields": []},
        )
        assert resp.status_code == 201, f"Schema {i} creation failed: {resp.text}"

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/kv-schemas",
        json={"name": "one_too_many", "fields": []},
    )
    assert resp.status_code == 422
    assert str(MAX_KV_SCHEMAS) in resp.json()["detail"]


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_create(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    # Create a schema
    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["name"] == "product"
    assert data["description"] == "A product schema"
    assert len(data["fields"]) == 4

    # Read it back
    resp = await nucliadb_reader.get(f"/kb/{kbid}/kv-schemas/product")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["name"] == "product"
    assert data["fields"][0]["key"] == "color"
    assert data["fields"][0]["type"] == "text"
    assert data["fields"][0]["required"] is True
    assert data["fields"][2]["key"] == "in_stock"
    assert data["fields"][2]["required"] is False


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_create_duplicate_rejected(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201

    # Duplicate name must be rejected
    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 409


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_create_duplicate_keys_rejected(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    bad_schema = {
        "name": "bad",
        "fields": [
            {"key": "color", "type": "text"},
            {"key": "color", "type": "float"},  # duplicate key
        ],
    }
    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=bad_schema)
    assert resp.status_code == 422


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_list(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    # Empty at start
    resp = await nucliadb_reader.get(f"/kb/{kbid}/kv-schemas")
    assert resp.status_code == 200
    assert resp.json()["schemas"] == {}

    # Create two schemas
    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201

    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/kv-schemas",
        json={"name": "metadata", "description": "Generic metadata", "fields": []},
    )
    assert resp.status_code == 201

    # List returns both
    resp = await nucliadb_reader.get(f"/kb/{kbid}/kv-schemas")
    assert resp.status_code == 200
    schemas = resp.json()["schemas"]
    assert set(schemas.keys()) == {"product", "metadata"}
    assert schemas["product"]["description"] == "A product schema"
    assert schemas["metadata"]["fields"] == []


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_get_not_found(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_reader.get(f"/kb/{kbid}/kv-schemas/nonexistent")
    assert resp.status_code == 404


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_update(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201

    # Update description only
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/kv-schemas/product",
        json={"description": "Updated description"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["description"] == "Updated description"
    # Fields unchanged
    assert len(data["fields"]) == 4

    # Update fields only
    new_fields = [
        {"key": "color", "type": "text", "required": True},
        {"key": "weight", "type": "float", "required": False},
    ]
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/kv-schemas/product",
        json={"fields": new_fields},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["fields"]) == 2
    assert data["fields"][1]["key"] == "weight"
    # Description from previous update preserved
    assert data["description"] == "Updated description"

    # Verify via GET
    resp = await nucliadb_reader.get(f"/kb/{kbid}/kv-schemas/product")
    assert resp.status_code == 200
    assert resp.json()["fields"][1]["key"] == "weight"


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_update_not_found(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/kv-schemas/nonexistent",
        json={"description": "Doesn't matter"},
    )
    assert resp.status_code == 404


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_update_duplicate_keys_rejected(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201

    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/kv-schemas/product",
        json={
            "fields": [
                {"key": "color", "type": "text"},
                {"key": "color", "type": "float"},  # duplicate
            ]
        },
    )
    assert resp.status_code == 422


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_delete(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201

    # Delete it
    resp = await nucliadb_writer.delete(f"/kb/{kbid}/kv-schemas/product")
    assert resp.status_code == 204

    # Gone from GET
    resp = await nucliadb_reader.get(f"/kb/{kbid}/kv-schemas/product")
    assert resp.status_code == 404

    # Gone from list
    resp = await nucliadb_reader.get(f"/kb/{kbid}/kv-schemas")
    assert resp.status_code == 200
    assert "product" not in resp.json()["schemas"]


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_delete_not_found(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.delete(f"/kb/{kbid}/kv-schemas/nonexistent")
    assert resp.status_code == 404


@pytest.mark.deploy_modes("standalone")
async def test_kv_schema_name_is_immutable_after_create(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    """The schema name is set at creation and the URL path is the identifier.
    PUT only accepts description and fields — there is no 'name' field in UpdateKVSchema,
    so any attempt to pass a name in the update body is rejected by the model."""
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201

    # Passing 'name' in PUT body should be rejected (extra fields forbidden)
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/kv-schemas/product",
        json={"name": "renamed", "description": "Trying to rename"},
    )
    assert resp.status_code == 422

    # Original name still intact
    resp = await nucliadb_reader.get(f"/kb/{kbid}/kv-schemas/product")
    assert resp.status_code == 200
    assert resp.json()["name"] == "product"
