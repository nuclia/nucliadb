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
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

import nucliadb_models as models
from nucliadb_models.common import FieldTypeName
from nucliadb_models.resource import (
    ConversationFieldExtractedData,
    Error,
    ExtractedDataType,
    FileFieldExtractedData,
    LinkFieldExtractedData,
    TextFieldExtractedData,
)

if TYPE_CHECKING:  # pragma: no cover
    ValueType = models.FieldText | models.FieldFile | models.FieldLink | models.Conversation | None
else:
    # without Any, pydantic fails to anything as validate() fails using the Union
    ValueType = Any


class ResourceField(BaseModel):
    field_type: FieldTypeName
    field_id: str
    value: ValueType = None
    extracted: ExtractedDataType | None = None
    error: Error | None = None
    status: str | None = None
    errors: list[Error] | None = None


FIELD_NAME_TO_EXTRACTED_DATA_FIELD_MAP: dict[FieldTypeName, Any] = {
    FieldTypeName.TEXT: TextFieldExtractedData,
    FieldTypeName.FILE: FileFieldExtractedData,
    FieldTypeName.LINK: LinkFieldExtractedData,
    FieldTypeName.CONVERSATION: ConversationFieldExtractedData,
}
