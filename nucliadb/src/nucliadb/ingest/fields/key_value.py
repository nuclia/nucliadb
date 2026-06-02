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
from __future__ import annotations

from datetime import datetime
from typing import Any

from typing_extensions import assert_never

from nucliadb.ingest.fields.base import Field
from nucliadb_models.key_value import Range
from nucliadb_models.kv_schemas import KVFieldType, KVSchema, KVSchemaField
from nucliadb_protos.resources_pb2 import FieldKeyValue


class KeyValue(Field[FieldKeyValue]):
    pbklass = FieldKeyValue
    type: str = "k"

    async def set_value(self, payload: FieldKeyValue) -> None:
        await self.db_set_value(payload)

    async def get_value(self) -> FieldKeyValue | None:
        return await self.db_get_value()


def validate_kv_data(data: dict, schema: KVSchema) -> None:
    _validate_keys(data, schema)
    schema_fields = {f.key: f for f in schema.fields}
    for key, value in data.items():
        check_kv_type(schema.id, schema_fields[key], key, value)


def _validate_keys(data: dict, schema: KVSchema) -> None:
    schema_keys = {f.key for f in schema.fields}
    unknown = set(data.keys()) - schema_keys
    if unknown:
        raise ValueError(f"Unknown keys for schema {schema.id!r}: {sorted(unknown)}")
    missing = [f.key for f in schema.fields if f.required and f.key not in data]
    if missing:
        raise ValueError(f"Missing required keys for schema {schema.id!r}: {missing}")


def check_kv_type(schema_name: str, schema_field: KVSchemaField, key: str, value: object) -> None:
    expected = schema_field.type

    ok = False
    if expected is KVFieldType.TEXT:
        # Tantivy's JSON indexer auto-parses strings as DateTime only when
        # they parse as RFC 3339, which requires both a time component and a
        # timezone offset (Z or ±HH:MM)
        if schema_field.repeated and isinstance(value, list):
            ok = all((isinstance(x, str) and not is_datetime(x)) for x in value)
        else:
            ok = isinstance(value, str) and not is_datetime(value)
    elif expected is KVFieldType.INTEGER:
        if schema_field.range and isinstance(value, Range):
            ok = (isinstance(value.lower, int) and not isinstance(value.lower, bool)) and (
                isinstance(value.upper, int) and not isinstance(value.upper, bool)
            )
        else:
            ok = isinstance(value, int) and not isinstance(value, bool)
    elif expected is KVFieldType.FLOAT:
        if schema_field.range and isinstance(value, Range):
            ok = (isinstance(value.lower, (int, float)) and not isinstance(value.lower, bool)) and (
                isinstance(value.upper, (int, float)) and not isinstance(value.upper, bool)
            )
        else:
            ok = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif expected is KVFieldType.BOOLEAN:
        ok = isinstance(value, bool)
    elif expected is KVFieldType.DATE:
        if schema_field.range:
            if isinstance(value, Range):
                ok = is_datetime(value.lower) and is_datetime(value.upper)
            else:
                ok = False
        else:
            ok = is_datetime(value)
    else:
        assert_never(expected)
    if not ok:
        if expected is KVFieldType.TEXT and isinstance(value, str):
            raise ValueError(
                f"Key {key!r} in schema {schema_name!r} expects type 'text', but the value looks like "
                f"a date. Use a 'date' field type for date values."
            )
        raise ValueError(
            f"Key {key!r} in schema {schema_name!r} expects type {expected.value!r}, got {type(value).__name__}"
        )


def is_datetime(value: Any) -> bool:
    # Dates must be stored as RFC 3339, which requires both a time component and
    # a timezone offset (Z or ±HH:MM).
    if isinstance(value, datetime):
        ok = value.tzinfo is not None
    elif isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            ok = dt.tzinfo is not None
        except ValueError:
            ok = False  # not parseable as a date
    else:
        ok = False
    return ok
