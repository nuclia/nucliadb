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
from datetime import datetime
from typing import Any

from pydantic import (
    BaseModel,
    Field,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    TypeAdapter,
    model_serializer,
    model_validator,
)
from typing_extensions import Self

from nucliadb_models.utils import DateTime


class Range(BaseModel):
    """A closed-bound integer range [lower, upper]."""

    lower: int | float | DateTime = Field(..., description="Lower closed bound (inclusive)")
    upper: int | float | DateTime = Field(..., description="Upper closed bound (inclusive)")

    @model_validator(mode="after")
    def check_bounds(self) -> Self:
        # we don't want differing types in a expression with mutliple inequality
        # operators
        if type(self.lower) is type(self.upper):
            if not (self.lower < self.upper):  # type: ignore # ty doesn't understand we already checked this
                raise ValueError(
                    f"lower endpoint ({self.lower}) must be < than its upper endpoint ({self.upper})"
                )
        else:
            raise ValueError("`lower` and `upper` types must be the same")

        return self

    @model_serializer(mode="wrap")
    def serialize_model(
        self,
        handler: SerializerFunctionWrapHandler,
        info: SerializationInfo,
    ) -> dict[str, object]:
        if info.context == "proto":
            # special serialization for proto
            return {
                "min": self.lower,
                "max": self.upper,
            }
        else:
            # fallback to default serialization
            return handler(self)

    @model_validator(mode="before")
    @classmethod
    def parse_from_proto(cls, data: Any) -> Any:
        if isinstance(data, dict):
            range_min = data.pop("min", None)
            if range_min is not None:
                data["lower"] = range_min
            range_max = data.pop("max", None)
            if range_max is not None:
                data["upper"] = range_max
        return data


# Type alias for valid KV field values
KVValue = str | list[str] | int | float | bool | datetime | Range


class KeyValueField(BaseModel):
    """A key-value field value. The field id (key in the resource's key_values dict)
    must equal the schema name — enforcing one KV field per schema per resource."""

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
