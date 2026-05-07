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
from enum import Enum

from pydantic import BaseModel, Field, model_validator

# Type alias for valid KV field values
KVValue = str | int | float | bool

MAX_KV_SCHEMAS = 20
MAX_KV_SCHEMA_FIELDS = 50


class KVFieldType(str, Enum):
    TEXT = "text"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"


class KVSchemaField(BaseModel):
    key: str = Field(pattern=r"^[^/.]{1,64}$")
    type: KVFieldType
    description: str = ""
    required: bool = True


class KVSchema(BaseModel):
    name: str = Field(pattern=r"^[^/.]{1,64}$")
    description: str = ""
    fields: list[KVSchemaField] = Field(default_factory=list, max_length=MAX_KV_SCHEMA_FIELDS)

    @model_validator(mode="after")
    def check_unique_keys(self) -> "KVSchema":
        keys = [f.key for f in self.fields]
        if len(keys) != len(set(keys)):
            raise ValueError("Schema field keys must be unique")
        return self


class CreateKVSchema(BaseModel):
    name: str = Field(pattern=r"^[^/.]{1,64}$")
    description: str = ""
    fields: list[KVSchemaField] = Field(default_factory=list, max_length=MAX_KV_SCHEMA_FIELDS)

    @model_validator(mode="after")
    def check_unique_keys(self) -> "CreateKVSchema":
        keys = [f.key for f in self.fields]
        if len(keys) != len(set(keys)):
            raise ValueError("Schema field keys must be unique")
        return self


class UpdateKVSchema(BaseModel, extra="forbid"):
    description: str | None = None
    fields: list[KVSchemaField] | None = Field(default=None, max_length=MAX_KV_SCHEMA_FIELDS)

    @model_validator(mode="after")
    def check_unique_keys(self) -> "UpdateKVSchema":
        if self.fields is not None:
            keys = [f.key for f in self.fields]
            if len(keys) != len(set(keys)):
                raise ValueError("Schema field keys must be unique")
        return self


class KBKVSchemas(BaseModel):
    schemas: dict[str, KVSchema] = Field(default_factory=dict)
