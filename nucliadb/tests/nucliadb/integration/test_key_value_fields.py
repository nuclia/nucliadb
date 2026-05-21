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

PRODUCT_SCHEMA = {
    "name": "product",
    "description": "Product schema",
    "fields": [
        {"key": "color", "type": "text", "required": True},
        {"key": "price", "type": "float", "required": True},
        {"key": "in_stock", "type": "boolean", "required": False},
        {"key": "quantity", "type": "integer", "required": False},
        {"key": "launched_at", "type": "date", "required": False},
    ],
}

VALID_PRODUCT_DATA = {
    "schema_id": "product",
    "data": {
        "color": "red",
        "price": 12.5,
        "in_stock": True,
        "quantity": 3,
        "launched_at": "2024-01-15T00:00:00Z",
    },
}


@pytest.mark.deploy_modes("standalone")
async def test_kv_field_crud(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    """
    Covers: create resource with KV field inline, read back via GET resource,
    update via PATCH, set/get via PUT, update field to required-only (optional
    keys disappear), delete field (404 after delete).
    """
    kbid = standalone_knowledgebox

    # Setup: create schema
    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201, resp.text

    # --- Create resource with KV field inline ---
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Resource with KV",
            "key_values": {"product": VALID_PRODUCT_DATA},
        },
    )
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    # Read back via GET resource with field_type filter
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{rid}",
        params={"show": ["values"], "field_type": ["key_value"]},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert "key_values" in data["data"]
    assert "product" in data["data"]["key_values"]
    value = data["data"]["key_values"]["product"]["value"]
    assert value["color"] == "red"
    assert value["price"] == 12.5
    assert value["in_stock"] is True
    assert value["quantity"] == 3
    assert value["launched_at"] == "2024-01-15T00:00:00Z"

    # --- Update resource via PATCH ---
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{rid}",
        json={
            "key_values": {
                "product": {"schema_id": "product", "data": {"color": "blue", "price": 99.0}},
            }
        },
    )
    assert resp.status_code == 200, resp.text

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}/key_value/product")
    assert resp.status_code == 200, resp.text
    assert resp.json()["value"]["color"] == "blue"

    # --- Set/get via PUT on a fresh resource ---
    resp = await nucliadb_writer.post(f"/kb/{kbid}/resources", json={"title": "Second resource"})
    assert resp.status_code == 201, resp.text
    rid2 = resp.json()["uuid"]

    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/resource/{rid2}/key_value/product",
        json=VALID_PRODUCT_DATA,
    )
    assert resp.status_code == 201, resp.text

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid2}/key_value/product")
    assert resp.status_code == 200, resp.text
    value = resp.json()["value"]
    assert value["color"] == "red"
    assert value["price"] == 12.5
    assert value["in_stock"] is True
    assert value["quantity"] == 3
    assert value["launched_at"] == "2024-01-15T00:00:00Z"

    # --- Update field: only required keys; optional keys disappear ---
    resp = await nucliadb_writer.put(
        f"/kb/{kbid}/resource/{rid2}/key_value/product",
        json={"schema_id": "product", "data": {"color": "green", "price": 5.0}},
    )
    assert resp.status_code == 201, resp.text

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid2}/key_value/product")
    assert resp.status_code == 200, resp.text
    value = resp.json()["value"]
    assert value["color"] == "green"
    assert value["price"] == 5.0
    assert "in_stock" not in value
    assert "quantity" not in value

    # --- Delete field ---
    resp = await nucliadb_writer.delete(f"/kb/{kbid}/resource/{rid2}/key_value/product")
    assert resp.status_code == 204, resp.text

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid2}/key_value/product")
    assert resp.status_code == 404, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_kv_field_validation(
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    """
    Covers: unknown schema, unknown keys, missing required keys, wrong types,
    field name != schema_id — all must return 422.
    """
    kbid = standalone_knowledgebox

    # Setup: schema + resource
    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201, resp.text

    resp = await nucliadb_writer.post(f"/kb/{kbid}/resources", json={"title": "Validation target"})
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    base_url = f"/kb/{kbid}/resource/{rid}/key_value"

    # Unknown schema
    resp = await nucliadb_writer.put(
        f"{base_url}/nonexistent",
        json={"schema_id": "nonexistent", "data": {"color": "red"}},
    )
    assert resp.status_code == 422, resp.text

    # Unknown keys
    resp = await nucliadb_writer.put(
        f"{base_url}/product",
        json={"schema_id": "product", "data": {"color": "red", "price": 1.0, "unknown_key": "oops"}},
    )
    assert resp.status_code == 422, resp.text

    # Missing required key (price)
    resp = await nucliadb_writer.put(
        f"{base_url}/product",
        json={"schema_id": "product", "data": {"color": "red"}},
    )
    assert resp.status_code == 422, resp.text

    # Wrong type: price should be float, not string
    resp = await nucliadb_writer.put(
        f"{base_url}/product",
        json={"schema_id": "product", "data": {"color": "red", "price": "not-a-number"}},
    )
    assert resp.status_code == 422, resp.text

    # Wrong type: in_stock should be boolean, not integer
    resp = await nucliadb_writer.put(
        f"{base_url}/product",
        json={"schema_id": "product", "data": {"color": "red", "price": 1.0, "in_stock": 1}},
    )
    assert resp.status_code == 422, resp.text

    # Wrong type: quantity should be integer, not float
    resp = await nucliadb_writer.put(
        f"{base_url}/product",
        json={"schema_id": "product", "data": {"color": "red", "price": 1.0, "quantity": 1.5}},
    )
    assert resp.status_code == 422, resp.text

    # Wrong type: launched_at should be ISO date string, not an integer timestamp
    resp = await nucliadb_writer.put(
        f"{base_url}/product",
        json={"schema_id": "product", "data": {"color": "red", "price": 1.0, "launched_at": 1234567890}},
    )
    assert resp.status_code == 422, resp.text

    # Wrong type: launched_at must be a valid ISO string, not free text
    resp = await nucliadb_writer.put(
        f"{base_url}/product",
        json={
            "schema_id": "product",
            "data": {"color": "red", "price": 1.0, "launched_at": "not-a-date"},
        },
    )
    assert resp.status_code == 422, resp.text

    # Field name in URL must match schema_id in body
    resp = await nucliadb_writer.put(
        f"{base_url}/product",
        json={"schema_id": "other", "data": {"color": "red", "price": 1.0}},
    )
    assert resp.status_code == 422, resp.text


@pytest.mark.deploy_modes("standalone")
async def test_kv_field_filter(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    """
    Covers: search/filtering via filter_expression using KeyValueFilter.
    Tests exact match, float range, bool match, integer range, and AND combination.
    """
    kbid = standalone_knowledgebox

    # --- Setup: create schema ---
    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201, resp.text

    # --- Create resource 1: red, price=12.5, in_stock=True, quantity=3 ---
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Product Red",
            "texts": {"body": {"body": "product item", "format": "PLAIN"}},
            "key_values": {
                "product": {
                    "schema_id": "product",
                    "data": {
                        "color": "red",
                        "price": 12.5,
                        "in_stock": True,
                        "quantity": 3,
                        "launched_at": "2023-06-01T00:00:00Z",
                    },
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid1 = resp.json()["uuid"]

    # --- Create resource 2: blue, price=5.0, in_stock=False, quantity=10 ---
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Product Blue",
            "texts": {"body": {"body": "product item", "format": "PLAIN"}},
            "key_values": {
                "product": {
                    "schema_id": "product",
                    "data": {
                        "color": "blue",
                        "price": 5.0,
                        "in_stock": False,
                        "quantity": 10,
                        "launched_at": "2024-06-01T00:00:00Z",
                    },
                }
            },
        },
    )
    assert resp.status_code == 201, resp.text
    rid2 = resp.json()["uuid"]

    async def find_with_filter(filter_expression: dict) -> set:
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json={
                "query": "product item",
                "features": ["keyword"],
                "filter_expression": {
                    "key_value": filter_expression,
                },
            },
        )
        assert resp.status_code == 200, resp.text
        return set(resp.json()["resources"].keys())

    filters = [
        # BOOLEAN fields
        ("in_stock", "eq", True, {rid1}),
        ("in_stock", "eq", False, {rid2}),
        # INTEGER fields
        ("quantity", "eq", 3, {rid1}),
        ("quantity", "gte", 1, {rid1, rid2}),
        ("quantity", "gte", 5, {rid2}),
        ("quantity", "lte", 20, {rid1, rid2}),
        ("quantity", "lte", 5, {rid1}),
        # FLOAT fields
        ("price", "eq", 5, {rid2}),
        ("price", "eq", 5.0, {rid2}),
        ("price", "gte", 5.0, {rid1, rid2}),
        ("price", "gte", 5.01, {rid1}),
        ("price", "lte", 12.5, {rid1, rid2}),
        ("price", "lte", 4.99, set()),
        #  price (float) can be filtered by an int
        ("price", "eq", 5, {rid2}),
        ("price", "gte", 5, {rid1, rid2}),
        ("price", "lte", 5, {rid2}),
        # TEXT fields
        ("color", "eq", "black", set()),
        ("color", "eq", "blue", {rid2}),
        ("color", "eq", "red", {rid1}),
        # DATE fields
        ("launched_at", "eq", "2024-06-01T00:00:00Z", {rid2}),
        ("launched_at", "gte", "2024-01-01T00:00:00Z", {rid2}),
        ("launched_at", "lte", "2023-12-31T23:59:59Z", {rid1}),
    ]
    for key, op, value, expected in filters:
        resources = await find_with_filter(
            {
                "schema_id": "product",
                "key": key,
                op: value,
            }
        )
        assert resources == expected, (
            f"Unexpected match for `{key} {op} {value}`: matched {resources} instead of {expected}"
        )

    # --- AND: color=red AND in_stock=True → finds resource 1 only ---
    rids = await find_with_filter(
        {
            "and": [
                {
                    "schema_id": "product",
                    "key": "color",
                    "eq": "red",
                },
                {
                    "schema_id": "product",
                    "key": "in_stock",
                    "eq": True,
                },
            ]
        }
    )
    assert rid1 in rids, f"Expected rid1 in results for color=red AND in_stock=True, got {rids}"
    assert rid2 not in rids, f"Expected rid2 NOT in results for color=red AND in_stock=True, got {rids}"


@pytest.mark.deploy_modes("standalone")
async def test_kv_filter_schema_validation(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
):
    """Test key-value schema validations: invalid schema, field or invalid types
    (type in schema is not compatible with query types)

    """
    kbid = standalone_knowledgebox

    resp = await nucliadb_writer.post(f"/kb/{kbid}/kv-schemas", json=PRODUCT_SCHEMA)
    assert resp.status_code == 201, resp.text

    async def find_with_filter(filter_expression: dict) -> int:
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json={
                "query": "product item",
                "features": ["keyword"],
                "filter_expression": {
                    "key_value": filter_expression,
                },
            },
        )
        return resp.status_code

    status = await find_with_filter(
        {
            "schema_id": "nonexistent_schema",
            "key": "color",
            "eq": "red",
        }
    )
    assert status == 412

    status = await find_with_filter(
        {
            "schema_id": "product",
            "key": "nonexistent_key",
            "eq": "red",
        }
    )
    assert status == 412

    # Test schema validation for invalid combinations of schemaa field types and
    # query values
    for key, op, value in [
        # invalid types for a BOOLEAN field
        ("in_stock", "eq", "true"),
        ("in_stock", "eq", 10),
        ("in_stock", "gte", 10),
        ("in_stock", "lte", 10),
        ("in_stock", "eq", 3.5),
        ("in_stock", "gte", 3.5),
        ("in_stock", "lte", 3.5),
        ("in_stock", "gte", "2024-01-01T00:00:00Z"),
        ("in_stock", "lte", "2024-01-01T00:00:00Z"),
        # invalid types for an INTEGER field
        ("quantity", "eq", "10"),
        ("quantity", "eq", True),
        ("quantity", "eq", 3.5),
        ("quantity", "gte", 3.5),
        ("quantity", "lte", 3.5),
        ("quantity", "gte", "2024-01-01T00:00:00Z"),
        ("quantity", "lte", "2024-01-01T00:00:00Z"),
        # invalid types for an FLOAT field
        ("price", "eq", "3.5"),
        ("price", "eq", True),
        ("price", "gte", "2024-01-01T00:00:00Z"),
        ("price", "lte", "2024-01-01T00:00:00Z"),
        # invalid types for a TEXT field
        ("color", "eq", True),
        ("color", "eq", 10),
        ("color", "gte", 10),
        ("color", "lte", 10),
        ("color", "eq", 3.5),
        ("color", "gte", 3.5),
        ("color", "lte", 3.5),
        ("color", "gte", "2024-01-01T00:00:00Z"),
        ("color", "lte", "2024-01-01T00:00:00Z"),
        # invalid types for a DATE field
        ("launched_at", "eq", "today"),
        ("launched_at", "eq", True),
        ("launched_at", "eq", 10),
        ("launched_at", "gte", 10),
        ("launched_at", "lte", 10),
        ("launched_at", "eq", 3.5),
        ("launched_at", "gte", 3.5),
        ("launched_at", "lte", 3.5),
    ]:
        status = await find_with_filter(
            {
                "schema_id": "product",
                "key": key,
                op: value,
            }
        )
        assert status == 412, f"Expected validation error for `{key} {op} {value}`, got {status}"
