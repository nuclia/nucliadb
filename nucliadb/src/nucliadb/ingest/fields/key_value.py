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

from nucliadb.ingest.fields.base import Field
from nucliadb_models.kv_schemas import KVFieldType, KVSchema
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
        check_kv_type(schema.name, key, value, schema_fields[key].type)


def _validate_keys(data: dict, schema: KVSchema) -> None:
    schema_keys = {f.key for f in schema.fields}
    unknown = set(data.keys()) - schema_keys
    if unknown:
        raise ValueError(f"Unknown keys for schema {schema.name!r}: {sorted(unknown)}")
    missing = [f.key for f in schema.fields if f.required and f.key not in data]
    if missing:
        raise ValueError(f"Missing required keys for schema {schema.name!r}: {missing}")


def check_kv_type(schema_name: str, key: str, value: object, expected: KVFieldType) -> None:
    ok = False
    if expected is KVFieldType.TEXT:
        ok = isinstance(value, str)
    elif expected is KVFieldType.INTEGER:
        ok = isinstance(value, int) and not isinstance(value, bool)
    elif expected is KVFieldType.FLOAT:
        ok = isinstance(value, (int, float)) and not isinstance(value, bool)
    elif expected is KVFieldType.BOOLEAN:
        ok = isinstance(value, bool)
    if not ok:
        raise ValueError(
            f"Key {key!r} in schema {schema_name!r} expects type {expected.value!r}, got {type(value).__name__}"
        )
