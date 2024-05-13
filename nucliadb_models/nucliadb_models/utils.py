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

import json

import pydantic
from typing_extensions import Annotated


def validate_field_id(value, handler, info):
    try:
        return handler(value)
    except:
        raise ValueError(
            f"Invalid field_id: '{value}'. Slug must be a string with only "
            "letters, numbers, underscores, colons and dashes."
        )


FieldIdPattern = r"^[a-zA-Z0-9:_-]+$"


FieldIdString = Annotated[
    str,
    pydantic.StringConstraints(pattern=FieldIdPattern),
    pydantic.WrapValidator(validate_field_id),
]


def validate_slug(value, handler, info):
    try:
        return handler(value)
    except:
        raise ValueError(
            f"Invalid slug: '{value}'. Slug must be a string with only "
            "letters, numbers, underscores, colons and dashes."
        )


SlugString = Annotated[
    str,
    pydantic.StringConstraints(pattern=r"^[a-zA-Z0-9:_-]+$"),
    pydantic.WrapValidator(validate_slug),
]


def validate_json(value: str):
    try:
        json.loads(value)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid JSON") from exc


class InvalidFieldIdError(pydantic.PydanticUserError):
    code = "wrong_field_id"
    msg_template = (
        "Invalid field id: '{value}'. Field ids must be a string with only "
        "letters, numbers, underscores, colons and dashes."
    )
