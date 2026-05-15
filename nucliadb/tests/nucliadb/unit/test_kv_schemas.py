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
import pytest
from pydantic import ValidationError

from nucliadb.ingest.fields.key_value import validate_kv_data
from nucliadb_models.kv_schemas import MAX_KV_SCHEMA_FIELDS, KVFieldType, KVSchema, KVSchemaField

SCHEMA = KVSchema(
    name="product",
    fields=[
        KVSchemaField(key="color", type=KVFieldType.TEXT, required=True),
        KVSchemaField(key="price", type=KVFieldType.FLOAT, required=True),
        KVSchemaField(key="in_stock", type=KVFieldType.BOOLEAN, required=False),
        KVSchemaField(key="quantity", type=KVFieldType.INTEGER, required=False),
    ],
)

DATE_SCHEMA = KVSchema(
    name="event",
    fields=[
        KVSchemaField(key="name", type=KVFieldType.TEXT, required=True),
        KVSchemaField(key="ts", type=KVFieldType.DATE, required=True),
        KVSchemaField(key="end_ts", type=KVFieldType.DATE, required=False),
    ],
)


class TestValidateKvData:
    def test_valid_data_all_fields(self):
        validate_kv_data({"color": "red", "price": 12.5, "in_stock": True, "quantity": 3}, SCHEMA)

    def test_valid_data_required_only(self):
        validate_kv_data({"color": "red", "price": 12.5}, SCHEMA)

    def test_unknown_key_rejected(self):
        with pytest.raises(ValueError, match="Unknown keys"):
            validate_kv_data({"color": "red", "price": 1.0, "typo": "oops"}, SCHEMA)

    def test_missing_required_key_rejected(self):
        with pytest.raises(ValueError, match="Missing required"):
            validate_kv_data({"color": "red"}, SCHEMA)

    def test_missing_all_keys_rejected(self):
        with pytest.raises(ValueError, match="Missing required"):
            validate_kv_data({}, SCHEMA)

    def test_wrong_type_text_field(self):
        with pytest.raises(ValueError, match="expects type 'text'"):
            validate_kv_data({"color": 123, "price": 1.0}, SCHEMA)

    def test_date_string_rejected_in_text_field(self):
        # Rejected: Tantivy would auto-detect these as DateTime (RFC 3339 with timezone).
        with pytest.raises(ValueError, match="looks like a date"):
            validate_kv_data({"color": "2024-01-15T00:00:00Z", "price": 1.0}, SCHEMA)

    def test_date_string_with_offset_rejected_in_text_field(self):
        with pytest.raises(ValueError, match="looks like a date"):
            validate_kv_data({"color": "2024-01-15T00:00:00+02:00", "price": 1.0}, SCHEMA)

    def test_date_only_string_allowed_in_text_field(self):
        # Allowed: Tantivy keeps date-only strings as Str (not RFC 3339).
        validate_kv_data({"color": "2024-01-15", "price": 1.0}, SCHEMA)

    def test_naive_datetime_string_allowed_in_text_field(self):
        # Allowed: no timezone → Tantivy keeps as Str (not RFC 3339).
        validate_kv_data({"color": "2024-01-15T00:00:00", "price": 1.0}, SCHEMA)

    def test_wrong_type_float_field(self):
        with pytest.raises(ValueError, match="expects type 'float'"):
            validate_kv_data({"color": "red", "price": "not-a-number"}, SCHEMA)

    def test_wrong_type_boolean_field(self):
        # An integer is not a bool
        with pytest.raises(ValueError, match="expects type 'boolean'"):
            validate_kv_data({"color": "red", "price": 1.0, "in_stock": 1}, SCHEMA)

    def test_wrong_type_integer_field(self):
        # A float is not an integer
        with pytest.raises(ValueError, match="expects type 'integer'"):
            validate_kv_data({"color": "red", "price": 1.0, "quantity": 1.5}, SCHEMA)

    def test_bool_not_accepted_as_integer(self):
        # bool is a subclass of int in Python, must be rejected for integer fields
        with pytest.raises(ValueError, match="expects type 'integer'"):
            validate_kv_data({"color": "red", "price": 1.0, "quantity": True}, SCHEMA)

    def test_bool_not_accepted_as_float(self):
        # bool is a subclass of float in Python, must be rejected for float fields
        with pytest.raises(ValueError, match="expects type 'float'"):
            validate_kv_data({"color": "red", "price": True}, SCHEMA)

    def test_integer_accepted_as_float(self):
        # Integers are acceptable for float fields
        validate_kv_data({"color": "red", "price": 10}, SCHEMA)

    def test_valid_date_iso_datetime_string(self):
        validate_kv_data({"name": "launch", "ts": "2024-01-15T00:00:00Z"}, DATE_SCHEMA)

    def test_valid_date_iso_date_only_string(self):
        # Date-only ISO strings are valid (no time component)
        validate_kv_data({"name": "launch", "ts": "2024-01-15"}, DATE_SCHEMA)

    def test_valid_date_with_optional_end(self):
        validate_kv_data(
            {"name": "launch", "ts": "2024-01-15T00:00:00Z", "end_ts": "2024-01-20T00:00:00Z"},
            DATE_SCHEMA,
        )

    def test_invalid_date_string_rejected(self):
        with pytest.raises(ValueError, match="expects type 'date'"):
            validate_kv_data({"name": "launch", "ts": "not-a-date"}, DATE_SCHEMA)

    def test_non_string_rejected_for_date_field(self):
        # Timestamps as integers are not accepted; must be ISO strings
        with pytest.raises(ValueError, match="expects type 'date'"):
            validate_kv_data({"name": "launch", "ts": 1234567890}, DATE_SCHEMA)

    def test_bool_not_accepted_as_date(self):
        with pytest.raises(ValueError, match="expects type 'date'"):
            validate_kv_data({"name": "launch", "ts": True}, DATE_SCHEMA)


class TestKVSchemaModel:
    def test_max_fields_limit(self):
        with pytest.raises(ValidationError):
            KVSchema.model_validate(
                {
                    "name": "big",
                    "fields": [
                        {"key": f"f{i}", "type": "text"} for i in range(MAX_KV_SCHEMA_FIELDS + 1)
                    ],
                }
            )

    def test_at_max_fields_is_accepted(self):
        schema = KVSchema.model_validate(
            {
                "name": "full",
                "fields": [{"key": f"f{i}", "type": "text"} for i in range(MAX_KV_SCHEMA_FIELDS)],
            }
        )
        assert len(schema.fields) == MAX_KV_SCHEMA_FIELDS

    def test_duplicate_field_keys_rejected(self):
        with pytest.raises(ValidationError, match="unique"):
            KVSchema.model_validate(
                {
                    "name": "bad",
                    "fields": [
                        {"key": "color", "type": "text"},
                        {"key": "color", "type": "float"},
                    ],
                }
            )
