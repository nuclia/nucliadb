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
import json

from pydantic import BaseModel, Field, TypeAdapter

from nucliadb_models.kv_schemas import KVValue


class KeyValueField(BaseModel):
    """A key-value field value. The field id (key in the resource's key_values dict)
    must equal the schema name — enforcing one KV field per schema per resource."""

    schema_id: str = Field(
        ...,
        title="Schema ID",
        description="The name of the KV schema this field conforms to.",
    )
    data: dict[str, KVValue] = Field(
        default_factory=dict,
        title="Data",
        description="Key-value pairs conforming to the schema.",
    )

    def serialize_json_for_proto(self) -> str:
        """Serialize to JSON for storage purposes

        This includes special handling for specific fields like Range, that
        serialize differently in the REST API than internally

        """
        return json.dumps(self.model_dump(include={"data"}, context="proto").get("data", {}))

    @classmethod
    def parse_from_proto_json(cls, serialized: str) -> dict[str, KVValue]:
        """Parses the `data` portion of the field from the stored JSON
        representation, that differs from the representation in the REST API

        """
        return TypeAdapter(dict[str, KVValue]).validate_json(serialized)
